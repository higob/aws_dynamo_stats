const { createDynamodbClient, dbuex } = require('../../lib/aws/dynamodb');
const { getDateISO8601 } = require('../../lib/dateUtils');

class MailEventLeadsService {
  constructor({ log } = {}) {
    this.log = log;
    this.dynamodbClient = createDynamodbClient(
      process.env.DYNAMODB_EMAIL_EVENT_LEADS_TABLE_NAME
    );
  }

  async insertEmailEvent({ mail, eventName, chiropractor, lead }) {
    const guid = mail.id;

    // Skip if id is undefined
    if (!lead.id) {
      return;
    }

    const updateExpressions = dbuex.getUpdateExpression(
      {
        guid,
      },
      {
        event_name: eventName,
        email: mail.email,
        payload: JSON.stringify({
          received_at: mail.received_at,
        }),
        office_phone: chiropractor.office_phone,
        lead_id: lead.id,
        full_name: lead.full_name,
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

module.exports = MailEventLeadsService;
