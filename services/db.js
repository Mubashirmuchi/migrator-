const mysql = require("mysql2");

function createConnection(config) {
  return mysql.createConnection({
    ...config,
    multipleStatements: true,
  });
}

module.exports = { createConnection };
