const DatabaseService = require('./database-service');

class CallsService extends DatabaseService {
  constructor({ log, pool } = {}) {
    super({
      log,
      pool,
    });
  }

  /**
   * Return dialed calls within a time range
   *
   * @param {string} createdAt
   *
   * @return {array}
   */
  async getDialedCallsStartFrom(createdAt) {
    const query = `SELECT c.lead_phone, c.created, c.direction, c.sent_to, c.sent_from,
    c.duration, c.status, l.email, l.id as lead_id, l.full_name, business.office_phone as business_office_phone, c.call_sid
    FROM calls c
    INNER JOIN leads l on l.phone = c.lead_phone
    INNER JOIN chiropractors business ON business.office_phone = l.office_phone
    WHERE c.direction = 'inbound'
    AND c.status IN ('ringing', 'in-progress', 'busy', 'canceled', 'no-answer', 'failed')
    AND c.lead_phone = c.sent_from
    AND c.created > "${createdAt}" AND c.created < NOW()
    ORDER BY c.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getDialedCallsStartFrom]');

    return data.length ? data : null;
  }

  /**
   * Return answered calls within a time range
   *
   * @param {string} createdAt
   * @returns {array}
   */
  async getAnsweredCallsStartFrom(createdAt) {
    const query = `SELECT c.lead_phone, c.created, c.direction, c.sent_to, c.sent_from,
    c.duration, c.status, l.email, l.id as lead_id, l.full_name, business.office_phone as business_office_phone, c.call_sid
    FROM calls c
    INNER JOIN leads l on l.phone = c.lead_phone
    INNER JOIN chiropractors business ON business.office_phone = l.office_phone
    WHERE c.direction = 'inbound'
    AND c.status IN ('completed')
    AND c.lead_phone = c.sent_from
    AND c.created > "${createdAt}" AND c.created < NOW()
    ORDER BY c.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getAnsweredCallsStartFrom]');

    return data.length ? data : null;
  }

  /**
   * Return dialed calls from the oldest
   *
   * @returns {array}
   */
  async getDialedCallsByOldest() {
    const query = `SELECT c.lead_phone, c.created, c.direction, c.sent_to, c.sent_from,
    c.duration, c.status, l.email, l.id as lead_id, l.full_name, business.office_phone as business_office_phone, c.call_sid
    FROM calls c
    INNER JOIN leads l on l.phone = c.lead_phone
    INNER JOIN chiropractors business ON business.office_phone = l.office_phone
    WHERE c.direction = 'inbound'
    AND c.status IN ('ringing', 'in-progress', 'busy', 'canceled', 'no-answer', 'failed')
    AND c.lead_phone = c.sent_from
    ORDER BY c.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getDialedCallsByOldest]');

    return data.length ? data : null;
  }

  /**
   * Return answered calls from the oldest
   *
   * @returns {array}
   */
  async getAnsweredCallsByOldest() {
    const query = `SELECT c.lead_phone, c.created, c.direction, c.sent_to, c.sent_from,
    c.duration, c.status, l.email, l.id as lead_id, l.full_name, business.office_phone as business_office_phone, c.call_sid
    FROM calls c
    INNER JOIN leads l on l.phone = c.lead_phone
    INNER JOIN chiropractors business ON business.office_phone = l.office_phone
    WHERE c.direction = 'inbound'
    AND c.status IN ('completed')
    AND c.lead_phone = c.sent_from
    ORDER BY c.created ASC
    LIMIT 500`;

    const data = await this.execute(query);

    this.log.info({ total: data.length }, '[getAnsweredCallsByOldest]');

    return data.length ? data : null;
  }
}

module.exports = CallsService;
