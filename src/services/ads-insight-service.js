/* eslint-disable  no-plusplus, no-async-promise-executor, no-await-in-loop */
const util = require('util');

const axios = require('axios');
const mysql = require('mysql');
const { makeDb } = require('mysql-async-simple');

const dbConfig = require('../configs/db');
const { getValue: getSecretValue } = require('../lib/aws/secret');

const { all_ad_accounts, all_ad_platform } = require('./queries');

const requestGet = async (axiosConf) => {
  const recursiveGet = async ({ url, params }, arr) => {
    const { data: result } = await axios.get(url, {
      params,
    });

    const { data, paging } = result;

    const results = arr.concat(data);
    if (paging && paging.next) {
      return recursiveGet({ url: paging.next }, results);
    }

    return results;
  };

  const aggregateInsights = (rawInsights) =>
    rawInsights.reduce((acc, crr) => {
      const {
        ad_id,
        publisher_platform,
        date_start,
        spend = 0,
        impressions = 0,
        reach = 0,
        clicks = 0,
        inline_link_clicks = 0,
      } = crr;

      // Group insights by ad_id, platform and date
      const key = ad_id + publisher_platform + date_start;

      const index = acc.findIndex((i) => i.key === key);

      // Update to the specific group if there is a similar ads
      if (index > -1) {
        acc[index].spend += parseFloat(spend);
        acc[index].impressions += parseInt(impressions, 10);
        acc[index].reach += parseInt(reach, 10);
        acc[index].clicks += parseInt(clicks, 10);
        acc[index].inline_link_clicks += parseInt(inline_link_clicks, 10);
        acc[index].ad_id = ad_id;
        acc[index].date_start = date_start;
      } else {
        acc.push({
          key,
          spend: parseFloat(spend),
          impressions: parseInt(impressions, 10),
          reach: parseInt(reach, 10),
          clicks: parseInt(clicks, 10),
          inline_link_clicks: parseInt(inline_link_clicks, 10),
          date_start,
          publisher_platform,
          ad_id,
        });
      }
      return acc;
    }, []);

  const r = await recursiveGet(axiosConf, []);

  const result = aggregateInsights(r);

  return result;
};

class AdsInsightService {
  constructor({ log, db } = {}) {
    this.log = log;
    this.dbConfig = db || dbConfig;
    this.db = makeDb();
    this.connection = null;
    this.tableName = 'ad_accounts';
    this.prosimifyQuery = null;
    this.fbGraphToken = null;
    this.platforms = [];
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

  async Initialize() {
    this.log.info('[Initialize] Running...');
    const awsSecrets = await getSecretValue(
      `fb/access-token/${process.env.SLS_STAGE}`
    );

    this.fbGraphToken = awsSecrets.FB_GRAPH_TOKEN;

    const connection = await this.getConnection();

    // Prosimify query function
    this.prosimifyQuery = util.promisify(connection.query).bind(connection);

    // Fetch platforms from database
    await this.FetchPlatforms();

    const adAccounts = await this.GetAllAdAccounts();

    await this.BatchUpdate(adAccounts).finally(async () => {
      await this.UpdateSnsInsights();

      if (this.connection != null) {
        await this.db.close(connection);
        this.connection = null;
      }
    });
  }

  async GetAllAdAccounts() {
    const data = await this.prosimifyQuery(all_ad_accounts);

    this.log.info(`[GetAllAdAccounts] Total Rows ${data.length}`);

    return data;
  }

  async FetchPlatforms() {
    this.platforms = await this.prosimifyQuery(all_ad_platform);
  }

  async BatchUpdate(adAccounts) {
    const chunkSize = 20;
    const totalBatches = Math.ceil(adAccounts.length / chunkSize);

    while (adAccounts.length > 0) {
      const batch = adAccounts.splice(0, chunkSize);

      this.log.info(
        `[BatchUpdate] Batch no: ${
          totalBatches - Math.ceil(adAccounts.length / chunkSize)
        } START`
      );

      await Promise.all(batch.map(this.InsightUpdateJob, this))
        .then(() =>
          this.log.info(
            `[BatchUpdate] Batch no: ${
              totalBatches - Math.ceil(adAccounts.length / chunkSize)
            } DONE`
          )
        )
        .catch((err) => this.log.error({ err }, "['BatchUpdate'] Error:"));
    }
  }

  InsightUpdateJob({ ad_account_id }) {
    return new Promise(async (resolve, reject) => {
      try {
        const insights = await this.GetInsightsFromFacebookGraphApi({
          ad_account_id,
        });

        for (let index = 0; index < insights.length; index++) {
          await this.PerformUpdate(insights[index]);
        }

        resolve(true);
      } catch (err) {
        reject(err);
      }
    });
  }

  async PerformUpdate(insight) {
    this.log.info('[PerformUpdate] Running...');

    const {
      ad_id,
      date_start,
      publisher_platform,
      impressions,
      reach,
      spend,
      clicks,
      inline_link_clicks,
    } = insight;
    const platformId = this.GetPlatformId(publisher_platform);

    if (platformId) {
      const query = `INSERT INTO ads_insights
        (spend, impressions, reach, clicks, inline_link_clicks, ad_id, date, platform)
        VALUES
        (${spend},${impressions},${reach},${clicks},${inline_link_clicks},'${ad_id}','${date_start}',${platformId})
        ON DUPLICATE KEY UPDATE
        spend = ${spend},
        impressions = ${impressions},
        reach = ${reach},
        clicks = ${clicks},
        inline_link_clicks = ${inline_link_clicks}`;

      await this.prosimifyQuery(query);

      this.log.info('[PerformUpdate] DONE...');

      return true;
    }
    this.log.info({ publisher_platform }, '[PerformUpdate] SKIP...');
    return false;
  }

  GetPlatformId(publisherPlatform) {
    const platform = this.platforms.find(
      (p) => p.name.toLowerCase() === publisherPlatform.toLowerCase()
    );

    if (platform) {
      return platform.id;
    }

    return null;
  }

  async GetInsightsFromFacebookGraphApi(adAccount) {
    try {
      this.log.info(
        { adAccountId: adAccount.ad_account_id },
        `[GetInsightsFromFacebookGraphApi] Running...`
      );

      const datePreset = 'this_month';
      const url = `${process.env.FB_GRAPH_API}/${adAccount.ad_account_id}/insights`;

      return requestGet({
        url,
        params: {
          access_token: this.fbGraphToken,
          level: 'ad',
          date_preset: datePreset,
          fields: 'spend,impressions,reach,clicks,inline_link_clicks,ad_id',
          breakdowns: 'publisher_platform,platform_position',
          time_increment: 1,
        },
      });
    } catch (err) {
      this.log.error({ err }, '[GetInsightsFromFacebookGraphApi] Error');
    }
    return null;
  }

  async UpdateSnsInsights() {
    try {
      await axios.post(process.env.API_INSIGHTS_UPDATE);

      this.log.info(`[UpdateSnsInsights] Called...`);
    } catch (err) {
      this.log.error({ err }, '[UpdateSnsInsights]');
    }
  }
}

module.exports = AdsInsightService;
