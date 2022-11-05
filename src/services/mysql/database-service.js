const util = require('util');

class DatabaseService {
  constructor({ log, pool } = {}) {
    this.log = log;
    this.connection = null;
    this.pool = pool;
  }

  /**
   * Initialize the mySQL pool connection
   */
  async initConnection() {
    this.pool.getConnection = util.promisify(this.pool.getConnection);
    this.connection = await this.pool.getConnection();
  }

  /**
   * Execute a query with async await
   *
   * @param {string} statement
   * @returns
   */
  async execute(statement) {
    try {
      this.connection.query = util.promisify(this.connection.query);
      const result = await this.connection.query(statement);

      return result;
    } catch (err) {
      this.log.error({ err }, '[execute]');
      this.connection.release();
      throw err;
    }
  }

  /**
   * Release the connection
   */
  async terminate() {
    this.connection.release();
  }
}

module.exports = DatabaseService;
