const { Client } = require('pg')
const fs = require('fs');

const queries = fs.readFileSync('./queries.sql').toString()

const dbs = [
  { // homologacao
    user: 'userhomolog',
    host: '00.123.456.789',
    database: 'dbhomolog',
    password: '123',
    port: 5432,
  },
  { // producao
    user: 'userprod',
    host: '123.456.789.00',
    database: 'dbprod',
    password: '123',
    port: 5432,
  },
  { // local
    user: 'teste',
    host: 'localhost',
    database: 'dblocal',
    password: '123',
    port: 5432,
  }
]

function executeQueryDb (dbConfig) {
  const client = new Client(dbConfig)
  
  client.connect()
  client.query(`${queries}`, [], (err, res) => {
    console.log(`(${dbConfig.host}):`, err ? err.stack : JSON.stringify(res))
    client.end()
  })
}

for (const db of dbs) {
  executeQueryDb(db)
}
