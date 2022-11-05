const mysql = require('mysql');
const { makeDb } = require('mysql-async-simple');

const {
  LEADS_STATS_TABLE,
  ROI_MONTHLY_TABLE,
  UPDATE_BATCH_SIZE,
} = require('../configs/db');
const { createDynamodbClient, dbuex } = require('../lib/aws/dynamodb');

const queries = require('./queries');

class StatsService {
  constructor({ log } = {}) {
    this.log = log;
    this.db = makeDb();
    this.connection = null;
  }

  async getConnection() {
    if (this.connection) {
      return this.connection;
    }

    const connection = mysql.createConnection({
      host: process.env.DBHOST,
      user: process.env.DBUSER,
      password: process.env.DBPASSWORD,
      database: process.env.DBNAME,
    });

    await this.db.connect(connection);

    this.connection = connection;
    return this.connection;
  }

  async Calculate() {
    this.log.info('Calculating leads statistics...');
    const connection = await this.getConnection();

    await this.CalculateTotals(queries.total_leads_query, 'total_leads');
    await this.CalculateTotals(
      queries.total_conversions_query,
      'total_conversions'
    );
    await this.CalculateTotals(
      queries.total_interactions_query,
      'total_interactions'
    );
    await this.CalculateTotals(queries.total_spend_query, 'total_spend');
    await this.CalculateTotals(queries.total_return_query, 'total_return');
    await this.CalculateMonthlyRoi(queries.roi_monthly_query);

    if (this.connection != null) {
      await this.db.close(connection);
      this.connection = null;
    }
  }

  async CalculateMonthlyRoi(query) {
    try {
      const dynamodbClient = createDynamodbClient(ROI_MONTHLY_TABLE);

      const data = await this.db.query(await this.getConnection(), query);

      const payloads = data.map((x) => {
        const { guid, mon, return: ret } = x;

        const updateExpression = dbuex.getUpdateExpression(
          {
            customer_id: guid,
            month: mon,
          },
          {
            return: ret,
          }
        );

        const payload = {
          Key: { customer_id: guid, month: mon },
          ...updateExpression,
        };

        return payload;
      });

      await dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);

      this.log.info(`updated RoI: ${data.length}`);
    } catch (err) {
      this.log.error({ err }, 'there was an error calculating monthly ROI');
    }
  }

  async CalculateTotals(query, type) {
    try {
      const dynamodbClient = createDynamodbClient(LEADS_STATS_TABLE);

      const data = await this.db.query(await this.getConnection(), query);

      const payloads = data.map((x) => {
        const { guid } = x;

        const updateExpression = dbuex.getUpdateExpression(
          { customer_id: guid },
          { [type]: x[type] }
        );

        const payload = {
          Key: { customer_id: guid },
          ...updateExpression,
        };

        return payload;
      });

      await dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);

      this.log.info(`updated ${type}: ${data.length}`);
    } catch (err) {
      this.log.error({ err }, `there was an error updating ${type}`);
    }
  }
}

module.exports = StatsService;
