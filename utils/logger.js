const fs = require("fs");
const LOG_FILE = "migration.log";

function write(line) {
  fs.appendFileSync(LOG_FILE, line + "\n");
}

function log(message) {
  const line = `[${new Date().toISOString()}] ${message}`;
  console.log(line);
  write(line);
}

function error(message) {
  const line = `[ERROR ${new Date().toISOString()}] ${message}`;
  console.error(line);
  write(line);
}

module.exports = { log, error };
