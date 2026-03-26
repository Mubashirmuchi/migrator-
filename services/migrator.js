// const fs = require("fs");
// const { createConnection } = require("./db");
// const { transform } = require("./transformer");
// const { log, error } = require("../utils/logger");

// async function migrate(config) {
//   const source = createConnection(config.source);
//   const target = createConnection(config.target);

//   let lastId = loadProgress();

//   log(`Starting from ID: ${lastId}`);

//   const query = source.query(config.query, [lastId]);

//   const BATCH_SIZE = config.batchSize || 1000;
//   let batch = [];
//   let count = 0;

//   query
//     .stream()
//     .on("data", (row) => {
//       const transformed = transform(row);

//       batch.push(Object.values(transformed));
//       lastId = row[config.idColumn];

//       if (batch.length >= BATCH_SIZE) {
//         query.pause();
//         insertBatch(target, config.targetTable, batch, () => {
//           saveProgress(lastId);
//           count += batch.length;
//           log(`Inserted: ${count}`);
//           batch = [];
//           query.resume();
//         });
//       }
//     })
//     .on("end", () => {
//       if (batch.length > 0) {
//         insertBatch(target, config.targetTable, batch, () => {
//           saveProgress(lastId);
//           log("✅ Migration completed");
//         });
//       } else {
//         log("✅ Migration completed");
//       }
//     })
//     .on("error", (err) => {
//       error(err.message);
//     });
// }

// function insertBatch(conn, table, rows, callback) {
//   if (rows.length === 0) return callback();

//   const sql = `INSERT INTO ${table} VALUES ?`;

//   conn.query(sql, [rows], (err) => {
//     if (err) {
//       error("Insert failed: " + err.message);
//     }
//     callback();
//   });
// }

// function loadProgress() {
//   if (!fs.existsSync("progress.json")) return 0;
//   const data = JSON.parse(fs.readFileSync("progress.json"));
//   return data.lastId || 0;
// }

// function saveProgress(lastId) {
//   fs.writeFileSync("progress.json", JSON.stringify({ lastId }));
// }

// module.exports = { migrate };

const fs = require("fs");
const cliProgress = require("cli-progress");
const { createConnection } = require("./db");
const { transform } = require("./transformer");
const { log, error } = require("../utils/logger");

const PROGRESS_FILE = "progress.json";

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE)) || {};
  } catch {
    return {};
  }
}

function saveProgress(data) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(data, null, 2));
}

async function getMinMax(source, table, idColumn) {
  return new Promise((resolve, reject) => {
    source.query(
      `SELECT MIN(${idColumn}) as min, MAX(${idColumn}) as max FROM ${table}`,
      (err, res) => {
        if (err) return reject(err);
        resolve(res[0]);
      },
    );
  });
}

async function migrate(config) {
  const source = createConnection(config.source);
  const target = createConnection(config.target);

  const progress = loadProgress();

  // Get min/max ID
  const { min, max } = await getMinMax(
    source,
    "employees", // change if needed
    config.idColumn,
  );

  const workers = config.workers || 1;
  const rangeSize = Math.ceil((max - min) / workers);

  console.log(`Total Range: ${min} → ${max}`);
  console.log(`Workers: ${workers}`);
  log(`Migration started — Range: ${min} → ${max}, Workers: ${workers}`);

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  bar.start(max - min, 0);

  let totalProcessed = 0;

  const workerPromises = [];

  for (let i = 0; i < workers; i++) {
    const start = min + i * rangeSize;
    const end = start + rangeSize;

    workerPromises.push(
      runWorker(i, start, end, source, target, config, progress, (count) => {
        totalProcessed += count;
        bar.update(totalProcessed);
      }),
    );
  }

  await Promise.all(workerPromises);

  bar.stop();
  log("✅ Migration completed");
  console.log("✅ Migration completed");
}

function runWorker(
  workerId,
  start,
  end,
  source,
  target,
  config,
  progress,
  onProgress,
) {
  return new Promise((resolve, reject) => {
    let lastId = progress[workerId] || start;

    const stream = source.query(config.query, [lastId, end]).stream();

    let batch = [];
    let processed = 0;

    stream
      .on("data", (row) => {
        const transformed = transform(row);
        batch.push(Object.values(transformed));
        lastId = row[config.idColumn];

        if (batch.length >= config.batchSize) {
          stream.pause();
          insertBatch(target, config.targetTable, batch, () => {
            processed += batch.length;
            onProgress(batch.length);
            progress[workerId] = lastId;
            saveProgress(progress);
            log(`Worker ${workerId} — inserted batch, lastId: ${lastId}, total: ${processed}`);
            batch = [];
            stream.resume();
          });
        }
      })
      .on("end", () => {
        if (batch.length > 0) {
          insertBatch(target, config.targetTable, batch, () => {
            onProgress(batch.length);
            progress[workerId] = lastId;
            saveProgress(progress);
            resolve();
          });
        } else {
          resolve();
        }
      })
      .on("error", (err) => {
        error(`Worker ${workerId} error: ${err.message}`);
        reject(err);
      });
  });
}

function insertBatch(conn, table, rows, callback) {
  if (!rows.length) return callback();

  conn.query(`INSERT INTO ${table} VALUES ?`, [rows], (err) => {
    if (err) error("Insert error: " + err.message);
    callback();
  });
}

module.exports = { migrate };
