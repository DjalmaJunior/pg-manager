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
	) LIMIT 1),
	(
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
	),
	parque.cod_centro_estoque_usados,
	aps.dth_inclusao,
	isa.*
FROM estoque.item_saida_atendimento isa
INNER JOIN ocorrencia.atendimento_ponto_servico aps
	ON aps.seq_atendimento_ponto_servico = isa.cod_atendimento_ponto_servico
INNER JOIN parque_servico.parque_servico parque
	ON parque.seq_parque_servico = aps.cod_parque_servico
WHERE isa.cod_versao_estoque IS NULL
AND (isa.desaparecido = FALSE OR isa.desaparecido IS NULL)
AND extract(MONTH FROM aps.dth_inclusao) > 6
AND EXTRACT(YEAR FROM aps.dth_inclusao) = 2023
ORDER BY isa.seq_item_saida_atendimento ASC;
