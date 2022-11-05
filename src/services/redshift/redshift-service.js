const util = require('util');

const Redshift = require('node-redshift-v2');

class RedshiftService {
  constructor({ log } = {}) {
    this.log = log;
    this.client = null;
    this.prosimifyQuery = null;
  }

  async getConnection() {
    if (this.client) {
      return this.client;
    }

    const client = new Redshift({
      user: process.env.RS_USERNAME,
      database: 'dev',
      password: process.env.RS_PASSWORD,
      port: 5439,
      host: process.env.RS_HOST,
    });

    this.client = client;

    return this.client;
  }

  async execute(query) {
    try {
      await this.getConnection();

      this.prosimifyQuery = util.promisify(this.client.query).bind(this.client);

      return this.prosimifyQuery(query);
    } catch (err) {
      this.log.error({ err }, '[RedshiftService,execute]');
      throw err;
    }
  }
}

module.exports = RedshiftService;
