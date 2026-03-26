const fs = require("fs");
const { migrate } = require("./services/migrator");

const configPath = process.argv[2];

if (!configPath) {
  console.error("Usage: node index.js config/client1.json");
  process.exit(1);
}

const config = JSON.parse(fs.readFileSync(configPath));

migrate(config);
