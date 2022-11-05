const RedshiftService = require('./redshift-service');

class SendGridService extends RedshiftService {
  constructor({ log } = {}) {
    super({ log });
  }

  async getOpenedEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid._open 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getOpenedEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid._open 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getClickedEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid.click 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE    
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getClickedEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid.click 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getDeliveredEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid.delivered 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE    
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getDeliveredEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid.delivered 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getSpamReportedEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid.spamreport 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE  
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getSpamReportedEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid.spamreport 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getUnsubscribedEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid.unsubscribe 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE    
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getUnsubscribedEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid.unsubscribe 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getGroupUnsubscribedEmailsStartFrom(receivedAt) {
    const query = `SELECT id, received_at, email FROM sendgrid.group_unsubscribe 
    WHERE received_at > '${receivedAt}' AND received_at < CURRENT_DATE
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }

  async getGroupUnsubscribedEmailsByOldest() {
    const query = `SELECT id, received_at, email FROM sendgrid.group_unsubscribe 
    ORDER BY received_at ASC
    LIMIT 100`;

    const { rows } = await this.execute(query);

    return rows || null;
  }
}

module.exports = SendGridService;
