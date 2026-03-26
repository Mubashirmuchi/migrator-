const fs = require("fs");
const cliProgress = require("cli-progress");
const { createConnection } = require("./db");
const { transform } = require("./transformer");
const { log, error } = require("../utils/logger");

function query(conn, sql, params) {
  return new Promise((resolve, reject) =>
    conn.query(sql, params, (err, res) => (err ? reject(err) : resolve(res)))
  );
}

async function migrate(config) {
  const source = createConnection(config.source);
  const target = createConnection(config.target);
  const id = config.idColumn;
  const srcTable = config.sourceTable;
  const dstTable = config.targetTable;
  const isMssql = config.target?.type === "mssql";

  const [srcRes] = await query(source, `SELECT MIN(${id}) as min, MAX(${id}) as max FROM ${srcTable}`);
  const [dstRes] = await query(target, `SELECT MAX(${id}) as max FROM ${dstTable}`);

  const srcMin = srcRes.min;
  const srcMax = srcRes.max;
  const dstMax = dstRes.max || 0;
  const start = dstMax > 0 ? dstMax + 1 : srcMin;

  if (start > srcMax) {
    console.log(`✅ No new records to migrate. (source max: ${srcMax}, destination max: ${dstMax})`);
    return;
  }

  console.log(`Migrating ${start} → ${srcMax} (${srcMax - start + 1} records)`);
  log(`Migration started — ${start} → ${srcMax}`);

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  bar.start(srcMax - start + 1, 0);

  const workers = config.workers || 1;
  const rangeSize = Math.ceil((srcMax - start + 1) / workers);

  await Promise.all(Array.from({ length: workers }, (_, i) => {
    const wStart = start + i * rangeSize;
    const wEnd = i === workers - 1 ? srcMax : wStart + rangeSize - 1;
    return runWorker(i, wStart, wEnd, source, target, config, isMssql, (n) => bar.increment(n));
  }));

  bar.stop();
  log("✅ Migration completed");
  console.log("✅ Migration completed");
}

function runWorker(workerId, start, end, source, target, config, isMssql, onProgress) {
  return new Promise((resolve, reject) => {
    const stream = source.query(config.query, [start, end]).stream();
    let batch = [], columns = null;

    stream
      .on("data", (row) => {
        const t = transform(row);
        if (!columns) columns = Object.keys(t);
        batch.push(Object.values(t));

        if (batch.length >= config.batchSize) {
          stream.pause();
          insertBatch(target, config.targetTable, batch, columns, isMssql, (n) => {
            onProgress(n);
            log(`Worker ${workerId} — batch inserted up to id ${row[config.idColumn]}`);
            batch = [];
            stream.resume();
          });
        }
      })
      .on("end", () => {
        if (batch.length > 0) {
          insertBatch(target, config.targetTable, batch, columns, isMssql, (n) => {
            onProgress(n);
            resolve();
          });
        } else {
          resolve();
        }
      })
      .on("error", (err) => { error(`Worker ${workerId}: ${err.message}`); reject(err); });
  });
}

function insertBatch(conn, table, rows, columns, isMssql, callback) {
  if (!rows.length) return callback(0);

  if (isMssql) {
    conn.bulkInsert(table, columns, rows)
      .then(() => callback(rows.length))
      .catch(err => { error("Insert error: " + err.message); callback(0); });
  } else {
    conn.query(`INSERT IGNORE INTO ${table} VALUES ?`, [rows], (err) => {
      if (err) error("Insert error: " + err.message);
      callback(rows.length);
    });
  }
}

module.exports = { migrate };
