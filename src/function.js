const { getValue: getSecretValue } = require('./lib/aws/secret');
const AdsInsightService = require('./services/ads-insight-service');
const AssetsStatsService = require('./services/assets-stats-service');
const { createLogger } = require('./services/log-service');
const StatsService = require('./services/stats-service');

const getEventTypeValue = (attributes) => {
  const { event_type = {} } = attributes;

  return event_type.stringValue || '';
};

module.exports.handler = async (event) => {
  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );
  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const log = createLogger({ handler: 'main-handler' });
  const statsService = new StatsService({ log });
  const assetStatsService = new AssetsStatsService({ log });

  log.info({ event }, 'event received');

  const { Records } = event;

  const promises = Promise.all(
    Records.map((r) => {
      const { body, messageAttributes } = r;
      const eventType = getEventTypeValue(messageAttributes);

      if (eventType === 'employee-profile-changed') {
        const payload = JSON.parse(body);

        return assetStatsService.updateEmployeeProfileOnAsset(payload);
      }

      if (eventType === 'chiropractor-profile-changed') {
        const payload = JSON.parse(body);

        return assetStatsService.updateChiropractorProfileOnAsset(payload);
      }

      if (eventType === 'ad-creatives-changed') {
        const { ad_account_id } = JSON.parse(body);

        return assetStatsService.calculateByAdAccountId(ad_account_id);
      }

      return Promise.all([
        assetStatsService.Calculate(),
        statsService.Calculate(),
      ]);
    })
  );

  const resp = await promises;
  log.info({ resp }, 'Done');
};

module.exports.adsInsightsHandler = async () => {
  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );

  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const log = createLogger({ handler: 'ad-insight-handler' });
  const adsInsightService = new AdsInsightService({ log });

  return adsInsightService.Initialize();
};
