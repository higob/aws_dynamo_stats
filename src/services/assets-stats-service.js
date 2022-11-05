/* eslint-disable no-nested-ternary */
const mysql = require('mysql');
const { makeDb } = require('mysql-async-simple');

const {
  UPDATE_BATCH_SIZE,
  ASSETS_STATS_TABLE,
  GENERIC_ASSETS_STATS_TABLE_NAME,
} = require('../configs/db');
const { createDynamodbClient, dbuex } = require('../lib/aws/dynamodb');
const { getUnixTimestamp, formatUTCDate } = require('../lib/dateUtils');

const queries = require('./queries');

const getLastUsedDate = ({ last_used, created_at }) =>
  last_used
    ? formatUTCDate(last_used)
    : created_at
    ? formatUTCDate(created_at)
    : 'Unknown';

class AssetsStatsService {
  constructor({ log } = {}) {
    this.log = log;
    this.db = makeDb();
    this.connection = null;
    this.dynamodbClient = createDynamodbClient(ASSETS_STATS_TABLE);
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

  async getChiroByAdAccountId(adAccountId) {
    const query = `select c.* from ad_accounts aa
      inner join chiropractors c on c.office_phone=aa.office_phone
      where aa.ad_account_id=?`;

    const [data] = await this.db.query(await this.getConnection(), query, [
      adAccountId,
    ]);

    return data;
  }

  async Calculate(chiropractorId) {
    this.log.info('Calculating assets statistics...');
    const connection = await this.getConnection();

    await this.CalculateTotalsForNonGenericAssets(
      chiropractorId
        ? queries.calculate_all_non_generic_assets_statistics_by_chiropractor_id
        : queries.calculate_all_non_generic_assets_statistics,
      chiropractorId
    );
    await this.CalculateTotalsForGenericAssets(
      chiropractorId
        ? queries.calculate_all_generic_assets_statistics_by_chiropractor_id
        : queries.calculate_all_generic_assets_statistics,
      chiropractorId
    );
    if (this.connection != null) {
      await this.db.close(connection);
      this.connection = null;
    }
  }

  async calculateByAdAccountId(adAccountId) {
    const chiro = await this.getChiroByAdAccountId(adAccountId);

    if (chiro) {
      console.log({ chiro });
      await this.Calculate(chiro.guid);
    }
  }

  async CalculateTotalsForNonGenericAssets(qury, chiropractorId) {
    try {
      const data = await this.db.query(await this.getConnection(), qury, [
        chiropractorId,
      ]);
      this.log.info(`Total Rows for primary assets ${data.length}`);

      const payloads = data.map((x) => {
        const {
          dashboard_uuid,
          guid,
          id,
          key_url,
          type,
          full_name,
          owner,
          office,
          last_used,
          created_at,
        } = x;

        const uploader_name =
          full_name && full_name.includes('sysuser') ? owner : full_name;

        const lastUsedUtc = getLastUsedDate({ last_used, created_at });
        const sort_by_date =
          lastUsedUtc === 'Unknown'
            ? getUnixTimestamp('2020-07-01T00:00:00.000Z')
            : getUnixTimestamp(lastUsedUtc);

        const updateExpression = dbuex.getUpdateExpression(
          {
            chiropractor_guid: guid,
            asset_id: Number(id),
          },
          {
            key_url,
            type,
            dashboard_uuid,
            user_name: uploader_name || office || 'Unknown',
            is_generic: 0,
            office: office || 'Unknown',
            created_at: x.created_at || '2020-07-01T00:00:00.000Z',
            num_creatives: Number(x.num_creatives || '0'),
            num_ads: Number(x.num_ads || '0'),
            ad_spend: Number(x.ad_spend || '0'),
            views: Number(x.views || '0'),
            views_facebook: Number(x.views_facebook || '0'),
            views_instagram: Number(x.views_instagram || '0'),
            views_messenger: Number(x.views_messenger || '0'),
            views_audience_network: Number(x.views_audience_network || '0'),
            lctr: Number(x.lctr || '0'),
            cpm: Number(x.cpm || '0'),
            leads: Number(x.leads || '0'),
            cost_per_lead: Number(x.cost_per_lead || '0'),
            shows: Number(x.shows || '0'),
            cost_per_show: Number(x.cost_per_show || '0'),
            last_used: String(lastUsedUtc),
            sort_by_date: Number(sort_by_date),
            disabled: x.disabled || 0,
          }
        );

        const payload = {
          Key: { chiropractor_guid: guid, asset_id: Number(id) },
          ...updateExpression,
        };

        return payload;
      });

      await this.dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);

      this.log.info(`updated all stats for primary assets: ${data.length}`);
    } catch (err) {
      this.log.error(
        { err },
        `there was an error on CalculateTotalsForNonGenericAssets`
      );
    }
  }

  async CalculateTotalsForGenericAssets(qury, chiropractorId) {
    try {
      const dClient = createDynamodbClient(GENERIC_ASSETS_STATS_TABLE_NAME);

      const data = await this.db.query(await this.getConnection(), qury, [
        chiropractorId,
      ]);
      this.log.info(`Total Rows for generic assets ${data.length}`);

      const payloads = data.map((x) => {
        const {
          asset_id,
          name,
          type,
          chiropractor_id,
          key_url,
          views_facebook,
          views_instagram,
          views_messenger,
          views_audience_network,
          last_used,
          created_at,
          num_creatives,
          num_ads,
          ad_spend,
          views,
          lctr,
          cpm,
          leads,
          cost_per_lead,
          shows,
          cost_per_show,
          disabled,
        } = x;

        const updateExpression = dbuex.getUpdateExpression(
          {
            chiropractor_guid: chiropractor_id,
            asset_id: Number(asset_id),
          },
          {
            key_url,
            type,
            is_generic: 1,
            last_used: getLastUsedDate({ last_used, created_at }),
            name,
            num_creatives: Number(num_creatives || '0'),
            num_ads: Number(num_ads || '0'),
            ad_spend: Number(ad_spend || '0'),
            views: Number(views || '0'),
            views_facebook: Number(views_facebook || '0'),
            views_instagram: Number(views_instagram || '0'),
            views_messenger: Number(views_messenger || '0'),
            views_audience_network: Number(views_audience_network || '0'),
            lctr: Number(lctr || '0'),
            cpm: Number(cpm || '0'),
            leads: Number(leads || '0'),
            cost_per_lead: Number(cost_per_lead || '0'),
            shows: Number(shows || '0'),
            cost_per_show: Number(cost_per_show || '0'),
            disabled: Boolean(disabled || 0),
          }
        );

        const payload = {
          Key: {
            chiropractor_guid: chiropractor_id,
            asset_id: Number(asset_id),
          },
          ...updateExpression,
        };

        return payload;
      });

      await dClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);

      this.log.info(`updated all stats for generic assets: ${data.length}`);
    } catch (err) {
      this.log.error(
        { err },
        `there was an error on CalculateTotalsForGenericAssets`
      );
    }
  }

  async updateEmployeeProfileOnAsset(payload) {
    const { dashboard_uuid, fullname: user_name } = payload;

    const params = {
      IndexName: 'employee-uuid-index',
      KeyConditionExpression: 'dashboard_uuid = :dashboard_uuid',
      ExpressionAttributeValues: {
        ':dashboard_uuid': dashboard_uuid,
      },
    };

    const items = await this.dynamodbClient.query(params);

    if (items) {
      const payloads = items.map((x) => {
        const { chiropractor_guid, asset_id } = x;

        const updateExpression = dbuex.getUpdateExpression(
          {
            chiropractor_guid,
            asset_id,
          },
          {
            user_name,
          }
        );

        const p = {
          Key: {
            chiropractor_guid,
            asset_id,
          },
          ...updateExpression,
        };

        return p;
      });

      await this.dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);
    }

    return items;
  }

  async updateChiropractorProfileOnAsset(payload) {
    const { chiropractor_guid: chiropractorGuid, avatar } = payload;

    const params = {
      KeyConditionExpression: 'chiropractor_guid = :chiropractor_guid',
      ExpressionAttributeValues: {
        ':chiropractor_guid': chiropractorGuid,
      },
    };

    const items = await this.dynamodbClient.query(params);

    if (items) {
      const payloads = items.map((x) => {
        const { chiropractor_guid, asset_id } = x;

        const updateExpression = dbuex.getUpdateExpression(
          {
            chiropractor_guid,
            asset_id,
          },
          {
            avatar,
          }
        );

        const p = {
          Key: {
            chiropractor_guid,
            asset_id,
          },
          ...updateExpression,
        };

        return p;
      });

      await this.dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);
    }

    return items;
  }
}

module.exports = AssetsStatsService;
