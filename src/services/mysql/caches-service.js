const DatabaseService = require('./database-service');

class CachesService extends DatabaseService {
  constructor({ log, pool } = {}) {
    super({ log, pool });
  }

  async getCacheByModule(module) {
    const query = `SELECT * FROM caches WHERE module = "${module}"`;

    const data = await this.execute(query);

    return data.length ? data[0] : null;
  }

  async insertUpdateCacheByModule({ module, body }) {
    const query = `INSERT INTO caches (module, body) 
    VALUES('${module}', '${body}') 
    ON DUPLICATE KEY UPDATE    
    module='${module}', body='${body}'`;

    await this.execute(query);
  }
}

module.exports = CachesService;
