const DatabaseService = require('./database-service');

class MessagesService extends DatabaseService {
  constructor({ log, pool } = {}) {
    super({
      log,
      pool,
    });
  }

  async getMessagesByMessageSid(sid) {
    const query = `SELECT * FROM messages WHERE message_sid = "${sid}"`;

    const data = await this.execute(query);

    return data.length ? data : null;
  }

  /**
   * Get sent messages within a time range
   *
   * @param {string} createdAt
   * @returns {array|null}
   */
  async getSentMessagesStartFrom(createdAt) {
    const query = `SELECT m.lead_phone, m.sent_to, m.sent_from, m.status, m.direction, m.body, m.message_sid,
     l.email, l.id as lead_id, l.full_name, c.office_phone AS business_office_phone, m.created
     FROM messages m 
     INNER JOIN leads l ON l.phone = m.lead_phone
     INNER JOIN chiropractors c ON c.office_phone = l.office_phone
     WHERE m.lead_phone = m.sent_from
     AND m.created > "${createdAt}" AND m.created < NOW()
     ORDER BY m.created ASC
     LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getSentMessagesStartFrom]');

    return data.length ? data : null;
  }

  /**
   * Return sent messages from the oldest `created`
   *
   * @returns {array|null}
   */
  async getSentMessagesByOldest() {
    const query = `SELECT m.lead_phone, m.sent_to, m.sent_from, m.status, m.direction, m.body, m.message_sid,
    l.email, l.id as lead_id, l.full_name, c.office_phone AS business_office_phone, m.created
    FROM messages m 
    INNER JOIN leads l ON l.phone = m.lead_phone
    INNER JOIN chiropractors c ON c.office_phone = l.office_phone
    WHERE m.lead_phone = m.sent_from
    ORDER BY m.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getSentMessagesByOldest]');

    return data.length ? data : null;
  }

  /**
   * Return received messages within a time range
   *
   * @param {string} createdAt
   * @returns {array}
   */
  async getReceivedMessagesStartFrom(createdAt) {
    const query = `SELECT m.lead_phone, m.sent_to, m.sent_from, m.status, m.direction, m.body, m.message_sid,
     l.email, l.id as lead_id, l.full_name, c.office_phone AS business_office_phone, m.created
     FROM messages m 
     INNER JOIN leads l ON l.phone = m.lead_phone
     INNER JOIN chiropractors c ON c.office_phone = l.office_phone
     WHERE m.lead_phone = m.sent_to
     AND m.created > "${createdAt}" AND m.created < NOW()
     ORDER BY m.created ASC
     LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getReceivedMessagesStartFrom]');

    return data.length ? data : null;
  }

  /**
   * Return received messages from the oldest `created`
   *
   * @returns {array}
   */
  async getReceivedMessagesByOldest() {
    const query = `SELECT m.lead_phone, m.sent_to, m.sent_from, m.status, m.direction, m.body, m.message_sid,
    l.email, l.id as lead_id, l.full_name, c.office_phone AS business_office_phone, m.created
    FROM messages m 
    INNER JOIN leads l ON l.phone = m.lead_phone
    INNER JOIN chiropractors c ON c.office_phone = l.office_phone
    WHERE m.lead_phone = m.sent_to
    ORDER BY m.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getReceivedMessagesByOldest]');

    return data.length ? data : null;
  }
}

module.exports = MessagesService;
