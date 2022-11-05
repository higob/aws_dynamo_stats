const { UPDATE_BATCH_SIZE } = require('../../configs/db');
const { createDynamodbClient, dbuex } = require('../../lib/aws/dynamodb');

class BusinessOffersService {
  constructor({ log } = {}) {
    this.log = log;
    this.dynamodbClient = createDynamodbClient(
      process.env.DYNAMODB_BUSINESS_OFFER_TABLE_NAME
    );
  }

  async batchUpdate(data) {
    const payloads = data.map((x) => {
      const { chiropractor_id, offer_id, name } = x;

      const updateExpression = dbuex.getUpdateExpression(
        {
          chiropractor_guid: chiropractor_id,
          offer_id: Number(offer_id),
        },
        {
          name,
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
          disabled: x.disabled || 0,
        }
      );

      const payload = {
        Key: { chiropractor_guid: chiropractor_id, offer_id: Number(offer_id) },
        ...updateExpression,
      };

      return payload;
    });

    await this.dynamodbClient.batchUpdate(payloads, UPDATE_BATCH_SIZE);
  }
}

module.exports = BusinessOffersService;
