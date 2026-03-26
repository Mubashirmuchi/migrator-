const { createConnection } = require("../services/db");
const config = require("../config/client1.json");

async function seed() {
  const conn = createConnection({ ...config.source, database: undefined });

  const run = (sql, values) =>
    new Promise((res, rej) =>
      conn.query(sql, values, (err, result) => (err ? rej(err) : res(result)))
    );

  await run(`CREATE DATABASE IF NOT EXISTS \`${config.source.database}\``);
  await run(`USE \`${config.source.database}\``);

  await run(`
    CREATE TABLE IF NOT EXISTS employees (
      emp_no INT PRIMARY KEY AUTO_INCREMENT,
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      department VARCHAR(50),
      salary DECIMAL(10,2),
      hired_at DATE
    )
  `);

  await run(`CREATE OR REPLACE VIEW emp_view AS SELECT * FROM employees`);

  const rows = Array.from({ length: 500 }, (_, i) => [
    `First${i + 1}`,
    `Last${i + 1}`,
    ["Engineering", "Sales", "HR", "Finance"][i % 4],
    (40000 + (i % 60) * 1000).toFixed(2),
    "2023-01-01",
  ]);

  await run(
    `INSERT INTO employees (first_name, last_name, department, salary, hired_at) VALUES ?`,
    [rows]
  );

  console.log(`✅ Seeded 500 rows into ${config.source.database}.employees`);
  conn.end();
}

seed().catch((err) => {
  console.error("Seed failed:", err.message);
  process.exit(1);
});
