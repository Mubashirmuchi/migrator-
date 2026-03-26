const { createConnection } = require("../services/db");
const config = require("../config/client2.json");

const db = config.target.database;

async function setup() {
  const conn = createConnection({ ...config.target, database: "master" });

  const run = (sql) =>
    new Promise((res, rej) =>
      conn.query(sql, (err, result) => (err ? rej(err) : res(result)))
    );

  await run(`IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '${db}') CREATE DATABASE [${db}]`);

  await run(`
    IF NOT EXISTS (SELECT * FROM [${db}].sys.objects WHERE name='${config.targetTable}' AND type='U')
    CREATE TABLE [${db}].dbo.${config.targetTable} (
      emp_no INT PRIMARY KEY,
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      department VARCHAR(50),
      salary DECIMAL(10,2),
      hired_at DATE
    )
  `);

  console.log(`✅ Target DB '${db}' and table '${config.targetTable}' ready`);
}

setup().catch((err) => {
  console.error("Setup failed:", err.message);
  process.exit(1);
});
