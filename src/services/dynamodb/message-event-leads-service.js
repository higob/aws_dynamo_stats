const { createDynamodbClient, dbuex } = require('../../lib/aws/dynamodb');
const { getDateISO8601 } = require('../../lib/dateUtils');

class MessageEventLeadsService {
  constructor({ log } = {}) {
    this.log = log;
    this.dynamodbClient = createDynamodbClient(
      process.env.DYNAMODB_MESSAGE_EVENT_LEADS_TABLE_NAME
    );
  }

  async insertMessageEvent({ chiropractor, lead, message, eventName }) {
    const guid = message.message_sid;

    const updateExpressions = dbuex.getUpdateExpression(
      {
        guid,
      },
      {
        office_phone: chiropractor.office_phone,
        lead_id: lead.id,
        event_name: eventName,
        payload: JSON.stringify({
          sent_to: message.sent_to,
          sent_from: message.sent_from,
          status: message.status,
          direction: message.direction,
          body: message.body,
          created: message.created,
        }),
        phone: message.lead_phone,
        full_name: lead.full_name,
        email: lead.email,
        created_at: getDateISO8601(),
        updated_at: getDateISO8601(),
      }
    );

    const params = {
      Key: { guid },
      ...updateExpressions,
    };

    await this.dynamodbClient.update(params);
  }
}

module.exports = MessageEventLeadsService;
