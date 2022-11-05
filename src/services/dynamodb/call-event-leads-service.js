const { createDynamodbClient, dbuex } = require('../../lib/aws/dynamodb');
const { getDateISO8601 } = require('../../lib/dateUtils');

class CallEventLeadsService {
  constructor({ log } = {}) {
    this.log = log;
    this.dynamodbClient = createDynamodbClient(
      process.env.DYNAMODB_CALL_EVENT_LEADS_TABLE_NAME
    );
  }

  async insertCallEvent({ chiropractor, lead, call, eventName }) {
    const guid = call.call_sid;

    const updateExpressions = dbuex.getUpdateExpression(
      {
        guid,
      },
      {
        office_phone: chiropractor.office_phone,
        lead_id: lead.id,
        event_name: eventName,
        payload: JSON.stringify({
          sent_to: call.sent_to,
          sent_from: call.sent_from,
          status: call.status,
          direction: call.direction,
          duration: call.duration,
          created: call.created,
        }),
        phone: call.lead_phone,
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

module.exports = CallEventLeadsService;
