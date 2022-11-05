const { getValue: getSecretValue } = require('./lib/aws/secret');
const BusinessOffersDynamodb = require('./services/dynamodb/business-offers-service');
const { createLogger } = require('./services/log-service');
const BusinessOffersService = require('./services/mysql/business-offers-service');
const pool = require('./services/mysql/connector');

module.exports.handler = async () => {
  const log = createLogger({ handler: 'business-offer-handler' });

  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );

  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const connectionPool = pool(process.env);
  const dynamodb = new BusinessOffersDynamodb({ log });
  const businessOffersService = new BusinessOffersService({
    log,
    pool: connectionPool,
  });

  try {
    await businessOffersService.initConnection();

    const results = await businessOffersService.getAllStats();

    await dynamodb.batchUpdate(results);
  } catch (err) {
    log.error({ err });
  } finally {
    await businessOffersService.terminate();
  }
};
