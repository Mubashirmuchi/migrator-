const { createConnection } = require("../services/db");
const config = require("../config/client2.json");

const db = config.source.database;

async function seed() {
  const conn = createConnection({ ...config.source, database: "master" });

  const run = (sql, params) =>
    new Promise((res, rej) =>
      conn.query(sql, params, (err, result) => (err ? rej(err) : res(result)))
    );

  await run(`IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '${db}') CREATE DATABASE [${db}]`);

  await run(`
    IF NOT EXISTS (SELECT * FROM [${db}].sys.objects WHERE name='employees' AND type='U')
    CREATE TABLE [${db}].dbo.employees (
      emp_no INT PRIMARY KEY IDENTITY(1,1),
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      department VARCHAR(50),
      salary DECIMAL(10,2),
      hired_at DATE
    )
  `);

  await run(`
    IF NOT EXISTS (SELECT * FROM [${db}].sys.views WHERE name = 'emp_view')
    EXEC sp_executesql N'USE [${db}]; EXEC(''CREATE VIEW emp_view AS SELECT * FROM employees'')'
  `);

  for (let i = 0; i < 500; i++) {
    await run(
      `INSERT INTO [${db}].dbo.employees (first_name, last_name, department, salary, hired_at) VALUES (?,?,?,?,?)`,
      [`First${i + 1}`, `Last${i + 1}`, ["Engineering", "Sales", "HR", "Finance"][i % 4], (40000 + (i % 60) * 1000).toFixed(2), "2023-01-01"]
    );
  }

  console.log(`✅ Seeded 500 rows into ${db}.employees`);
}

seed().catch((err) => {
  console.error("Seed failed:", err.message);
  process.exit(1);
});
