const { Pool } = require('pg')

const orNull = (value) => [null, undefined].includes(value) ? 'NULL' : value;

const queryDataToFix = /* sql */`
  SELECT
    (SELECT seq_estoque FROM estoque.estoque WHERE hash_agrupamento = (
      estoque.get_hash_agrupamento((
        WITH rec_dados AS (
          SELECT
            isa.cod_item,
            isa.cod_lote,
            parque.cod_centro_estoque_usados AS cod_centro_estoque,
            CASE WHEN isa.defeituoso THEN 4 ELSE 2 END AS cod_estado_material,
            FALSE AS is_insumo
        ) SELECT ROW_TO_JSON(rec_dados) FROM rec_dados
      ))
    ) LIMIT 1) AS seq_estoque,
    parque.cod_centro_estoque_usados,
    aps.dth_inclusao::text,
    isa.seq_item_saida_atendimento,
    isa.quantidade,
    isa.cod_item,
    isa.cod_lote,
    CASE WHEN isa.defeituoso THEN 4 ELSE 2 END AS cod_estado_material,
    parque.cod_centro_estoque_usados AS cod_centro_estoque
  FROM estoque.item_saida_atendimento isa
  INNER JOIN ocorrencia.atendimento_ponto_servico aps
    ON aps.seq_atendimento_ponto_servico = isa.cod_atendimento_ponto_servico
  INNER JOIN parque_servico.parque_servico parque
    ON parque.seq_parque_servico = aps.cod_parque_servico
  WHERE isa.cod_versao_estoque IS NULL
  AND (isa.desaparecido = FALSE OR isa.desaparecido IS NULL)
  AND extract(MONTH FROM aps.dth_inclusao) > 6
  AND EXTRACT(YEAR FROM aps.dth_inclusao) = 2023
  ORDER BY isa.seq_item_saida_atendimento ASC
  --LIMIT 1; -- LIMIT 1 PARA TESTAR
`

const queryVersoesEstoque = async (client, cod_estoque, dth_movimentacao) => client.query(/* sql */`
  WITH rec_versoes AS (
    SELECT
      ve.*
    FROM estoque.versao_estoque ve
    WHERE ve.cod_estoque = ${cod_estoque}
    AND ve.dth_inclusao > '${dth_movimentacao}'::timestamp
    ORDER BY ve.num_versao
  ),
  rec_versao_inicial_anterior AS (
    SELECT DISTINCT ON (ve.cod_estoque)
      ve.*
    FROM estoque.versao_estoque ve
    WHERE (ve.num_versao = (SELECT min(rec_versoes.num_versao) - 1 FROM rec_versoes) OR (SELECT min(rec_versoes.num_versao) - 1 FROM rec_versoes) IS NULL)
    AND ve.cod_estoque = ${cod_estoque}
    AND ve.dth_inclusao < '${dth_movimentacao}'::timestamp
    ORDER BY ve.cod_estoque, ve.num_versao DESC
  )
  SELECT
    rec_versao_inicial_anterior.*
  FROM rec_versao_inicial_anterior
  UNION
  SELECT
    rec_versoes.*
  FROM rec_versoes
  ORDER BY num_versao;
`)

const getSeqEstoqueGeradoNoProcesso = async (client, dados) => client.query(/* sql */`
 SELECT 
  seq_estoque 
 FROM estoque.estoque WHERE hash_agrupamento = (
    estoque.get_hash_agrupamento((
      WITH rec_dados AS (
        SELECT
          ${dados.cod_item},
          ${dados.cod_lote},
          ${dados.cod_centro_estoque},
          ${dados.cod_estado_material},
          FALSE AS is_insumo
      ) SELECT ROW_TO_JSON(rec_dados) FROM rec_dados
    ))
  ) 
  LIMIT 1
`)

const garantindoOrdemExistenteDeVersoes = async(client, seq_estoque) => client.query(/* sql */`
  SELECT estoque.remap_estoque_num_versao_order (${seq_estoque});
`)

module.exports = {
  async getDataToFix (dbConfig) {
    const pool = new Pool(dbConfig)
    
    const client = await pool.connect()
      
    const originalConsoleLog = console.log;
    console.log = function () {
      const args = [`(${dbConfig.host}):`, ...arguments]
    
      originalConsoleLog.apply(console, args);
    }
    
    const res = await client.query(`${queryDataToFix}`, [])
    
    for (const [indexRow, row] of res.rows.entries()) {
      try {
        await client.query('BEGIN')
        console.log(`Lendo linha ${indexRow + 1} de ${res.rows.length}...`)
  
        const dthMovimentacao = new Date(row.dth_inclusao + '+0000').toISOString()
        const quantidadeMovimentada = Number(row.quantidade)
        let modeloBase = {
          num_versao: 0,
          cod_item: row.cod_item,
          cod_lote: row.cod_lote,
          cod_estado_material: row.cod_estado_material,
          cod_centro_estoque: row.cod_centro_estoque,
          val_unitario: 1,
          num_quantidade_estoque: 0
        }
        let versoes = { rows: [] }
  
        // verificar se algum indice ja cadastrou os dados iniciais desse hash_agrupamento
        if (!row.seq_estoque) {
          const respSeq = await getSeqEstoqueGeradoNoProcesso(client, row)
  
          row.seq_estoque = respSeq.rows?.[0]?.seq_estoque
        }
  
        if (!!row.seq_estoque) {
          await garantindoOrdemExistenteDeVersoes(client, row.seq_estoque)
  
          versoes = await queryVersoesEstoque(client, row.seq_estoque, dthMovimentacao)
  
          console.log('versoes', JSON.stringify(versoes), row.seq_estoque, dthMovimentacao)

          modeloBase = versoes.rows[0]
        }
  
        const novaVersao = {
          dth_inclusao: dthMovimentacao,
          num_versao: modeloBase.num_versao + 1,
          cod_estoque: modeloBase.cod_estoque,
          cod_item: modeloBase.cod_item,
          cod_centro_estoque: modeloBase.cod_centro_estoque,
          num_quantidade_estoque: Number(modeloBase.num_quantidade_estoque) + quantidadeMovimentada,
          val_unitario: modeloBase.val_unitario,
          validade_produto: modeloBase.validade_produto,
          status_vencido: modeloBase.status_vencido || 'N',
          status_liberado: modeloBase.status_liberado || 'N',
          cod_codigo_barras_estoque: modeloBase.cod_codigo_barras_estoque,
          cod_lote: modeloBase.cod_lote,
          ind_uso: modeloBase.ind_uso || 'S',
          cod_estado_material: modeloBase.cod_estado_material,
          cod_serial: modeloBase.cod_serial,
          num_quantidade_estoque_reservada: modeloBase.num_quantidade_estoque_reservada,
          is_insumo: modeloBase.is_insumo || false,
          cod_estoque_pai: modeloBase.cod_estoque_pai,
          cod_dispositivo_telegestao: modeloBase.cod_dispositivo_telegestao,
          num_versao_atual: modeloBase.num_versao_atual ? modeloBase.num_versao_atual + 1 : null,
          hash_agrupamento: modeloBase.hash_agrupamento,
          cod_centro_estoque_localizacao: modeloBase.cod_centro_estoque_localizacao,
        }
  
        if (versoes.rows.length > 1) {
          const [first, ...rowsToUpdate] = versoes.rows
  
          var updates = []
  
          let qtdAnterior, qtdModificadaAnterior;
          for (const rowToUpdate of rowsToUpdate) {
            const diferencaQtdAnterior = Number(rowToUpdate.num_quantidade_estoque) - (qtdAnterior ?? Number(first.num_quantidade_estoque))
            let qtd = Number(rowToUpdate.num_quantidade_estoque) + diferencaQtdAnterior
  
            if (!diferencaQtdAnterior && !!qtdModificadaAnterior && qtdModificadaAnterior != qtd) {
              qtd = qtdModificadaAnterior
            }
  
            updates.push(`
              UPDATE estoque.versao_estoque
              SET 
                num_versao=${Number(rowToUpdate.num_versao)+1},
                num_versao_atual=${Number(rowToUpdate.num_versao_atual)+1},
                num_quantidade_estoque=${qtd}
              WHERE seq_versao_estoque=${rowToUpdate.seq_versao_estoque};
            `)
  
            qtdModificadaAnterior = qtd
            qtdAnterior = Number(rowToUpdate.num_quantidade_estoque)
          }
  
          updates.reverse()
        }
  
        let cod_versao_estoque_final
  
        if (!!row.seq_estoque) {
          const estoqueUpdated = await client.query(/* sql */`
            UPDATE estoque.estoque
            SET num_quantidade_estoque = num_quantidade_estoque + ${quantidadeMovimentada}
            WHERE seq_estoque = ${row.seq_estoque}
            RETURNING (num_versao_atual + 1) AS num_versao_atual;
          `)
  
          const num_versao_atual = estoqueUpdated.rows[0].num_versao_atual;
          const num_versao_provisoria = num_versao_atual + 1;
          
          console.log('\n'.repeat(3), num_versao_atual, num_versao_provisoria)
  
          await client.query(/* sql */`
            UPDATE estoque.versao_estoque
            SET num_versao = ${num_versao_provisoria}
            WHERE cod_estoque = ${row.seq_estoque}
            AND num_versao = ${num_versao_atual};
          `)

          if (!!updates?.length) {
            await client.query(updates.join(' '))
          }
  
          console.log('versaoUpdated', row.seq_estoque, JSON.stringify(novaVersao))

          const versaoUpdated = await client.query(/* sql */`
            UPDATE estoque.versao_estoque
            SET
              ${
                Object.keys(novaVersao)
                .map(key => `${key} = ${typeof novaVersao[key] === 'string' ? `'${novaVersao[key]}'` : orNull(novaVersao[key])}`)
                .join(',')
              }
            WHERE cod_estoque = ${row.seq_estoque}
            AND num_versao = ${num_versao_provisoria}
            RETURNING seq_versao_estoque;
          `)

          cod_versao_estoque_final = versaoUpdated.rows[0].seq_versao_estoque
        } else {
          const keysNotUsed = [
            'dth_inclusao',
            'num_versao',
            'cod_estoque',
            'num_versao_atual'
          ]
          const keys = Object.keys(novaVersao).filter(key => !keysNotUsed.includes(key))
  
          const estoqueCreated = await client.query(/* sql */`
            INSERT INTO estoque.estoque (${keys.join(',')})
            VALUES 
            (${
              keys
              .map(key => `${typeof novaVersao[key] === 'string' ? `'${novaVersao[key]}'` : orNull(novaVersao[key])}`)
              .join(',')
            })
            RETURNING seq_estoque
          `)
  
          const versaoCreated = await client.query(/* sql */`
            SELECT
              ve.seq_versao_estoque
            FROM estoque.versao_estoque ve
            WHERE ve.cod_estoque = ${estoqueCreated.rows[0].seq_estoque}
            LIMIT 1
          `)
  
          console.log('versaoCreated', estoqueCreated.rows[0].seq_estoque)
          cod_versao_estoque_final = versaoCreated.rows[0].seq_versao_estoque
          
          await client.query(/* sql */`
            UPDATE estoque.versao_estoque
            SET dth_inclusao = '${dthMovimentacao}'
            WHERE seq_versao_estoque = ${cod_versao_estoque_final};
          `)
        }
  
        if (!cod_versao_estoque_final) throw new Error('Não foi possível definir uma versão de estoque para atualizar a movimentação!')
  
        console.log('\n'.repeat(3), 'novaVersao:', JSON.stringify(novaVersao), '\n')
        
        await client.query(/* sql */`
          UPDATE estoque.item_saida_atendimento
          SET 
            cod_versao_estoque = ${cod_versao_estoque_final}
          WHERE seq_item_saida_atendimento = ${row.seq_item_saida_atendimento};
        `)
  
        await client.query('COMMIT')
      } catch (err) {
        await client.query('ROLLBACK')
        console.log(err.message, '\n'.repeat(3), err.stack)
      }
    }

    client.release()
    pool.end()
  }
}
