const mysql = require("mysql2");
const mssql = require("mssql");

function createConnection(config) {
  if (config.type === "mssql") return new MSSQLWrapper(config);
  return mysql.createConnection({ ...config, multipleStatements: true });
}

class MSSQLWrapper {
  constructor(config) {
    this.type = "mssql";
    this.config = {
      user: config.user,
      password: config.password,
      server: config.host,
      port: config.port || 1433,
      database: config.database,
      pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
      options: { encrypt: config.encrypt ?? false, trustServerCertificate: true },
    };
    this._pool = null;
    this._connecting = null;
  }

  _getPool() {
    if (this._pool && this._pool.connected) return Promise.resolve(this._pool);
    if (!this._connecting) {
      const pool = new mssql.ConnectionPool(this.config);
      this._connecting = pool.connect().then(p => {
        this._pool = p;
        return p;
      });
    }
    return this._connecting;
  }

  query(sql, params, callback) {
    if (typeof params === "function") { callback = params; params = []; }
    if (typeof callback === "function") {
      (async () => {
        try {
          const pool = await this._getPool();
          const request = pool.request();
          let i = 0;
          const msql = sql.replace(/\?/g, () => {
            const name = `p${++i}`;
            request.input(name, (params || [])[i - 1]);
            return `@${name}`;
          });
          const result = await request.query(msql);
          callback(null, result.recordset || []);
        } catch (err) { callback(err); }
      })();
      return;
    }

    const self = this;
    return {
      stream() {
        const { EventEmitter } = require("events");
        const emitter = new EventEmitter();
        emitter.pause = () => (emitter._paused = true);
        emitter.resume = () => { emitter._paused = false; emitter.emit("_resume"); };

        (async () => {
          try {
            const pool = await self._getPool();
            const request = pool.request();
            let i = 0;
            const msql = sql.replace(/\?/g, () => {
              const name = `p${++i}`;
              request.input(name, (params || [])[i - 1]);
              return `@${name}`;
            });
            const result = await request.query(msql);
            for (const row of result.recordset) {
              if (emitter._paused) await new Promise(res => emitter.once("_resume", res));
              emitter.emit("data", row);
            }
            emitter.emit("end");
          } catch (err) { emitter.emit("error", err); }
        })();

        return emitter;
      }
    };
  }

  async bulkInsert(table, columns, rows) {
    const pool = await this._getPool();
    const colList = columns.join(", ");
    const request = pool.request();

    const valueSets = rows.map((row, ri) =>
      `(${row.map((val, ci) => {
        const name = `v${ri}_${ci}`;
        request.input(name, val);
        return `@${name}`;
      }).join(",")})`
    ).join(",");

    const pkCol = columns[0];
    await request.query(`
      INSERT INTO ${table} (${colList})
      SELECT ${colList} FROM (VALUES ${valueSets}) AS src(${colList})
      WHERE NOT EXISTS (SELECT 1 FROM ${table} WHERE ${pkCol} = src.${pkCol})
    `);
  }
}

module.exports = { createConnection };
