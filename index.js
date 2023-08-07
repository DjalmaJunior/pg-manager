// const { Client } = require('pg')
// const fs = require('fs');

// const queries = fs.readFileSync('./queries.sql').toString()

const { dbs } = require('./dbs')
const { getDataToFix } = require('./tmp')

// function executeQueryDb (dbConfig) {
//   const client = new Client(dbConfig)
  
//   client.connect()
//   client.query(`${queries}`, [], (err, res) => {
//     console.log(`(${dbConfig.host}):`, err ? err.stack : JSON.stringify(res))
//     client.end()
//   })
// }

// for (const db of dbs) {
//   executeQueryDb(db)
// }

for (const db of dbs.filter(Boolean)) {
  getDataToFix(db)
}
