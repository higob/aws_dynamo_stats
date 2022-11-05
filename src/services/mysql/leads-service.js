const DatabaseService = require('./database-service');

class LeadsService extends DatabaseService {
  constructor({ log, pool } = {}) {
    super({ log, pool });
  }

  async getLeadByEmail(email) {
    const query = `SELECT l.email, c.office_phone, l.full_name, l.id
        FROM leads l
        INNER JOIN chiropractors c ON c.office_phone = l.office_phone
        WHERE l.email = "${email}"
        ORDER BY created DESC
        LIMIT 1`;

    const data = await this.execute(query);

    // To prevent overwhelming logs, temporarily disable this
    // this.log.debug("[getLeadByEmail]", {
    //     email,
    //     total: data.length
    // });

    return data.length ? data[0] : null;
  }
}

module.exports = LeadsService;
