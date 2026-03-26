const { createConnection } = require("../services/db");
const config = require("../config/client1.json");

async function setup() {
  const conn = createConnection({ ...config.target, database: undefined });

  const run = (sql) =>
    new Promise((res, rej) =>
      conn.query(sql, (err, result) => (err ? rej(err) : res(result)))
    );

  await run(`CREATE DATABASE IF NOT EXISTS \`${config.target.database}\``);
  await run(`USE \`${config.target.database}\``);

  await run(`
    CREATE TABLE IF NOT EXISTS ${config.targetTable} (
      emp_no INT PRIMARY KEY,
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      department VARCHAR(50),
      salary DECIMAL(10,2),
      hired_at DATE
    )
  `);

  console.log(`✅ Target DB '${config.target.database}' and table '${config.targetTable}' ready`);
  conn.end();
}

setup().catch((err) => {
  console.error("Setup failed:", err.message);
  process.exit(1);
});
