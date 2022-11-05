module.exports = {
  HOST: process.env.DBHOST,
  USER: process.env.DBUSER,
  DB: process.env.DBNAME,
  PASSWORD: process.env.DBPASSWORD,
  LEADS_STATS_TABLE:
    process.env.DYNAMODB_LEADS_STATS_TABLE_NAME || 'dev-leads-stats-dyn',
  ASSETS_STATS_TABLE:
    process.env.DYNAMODB_ASSETS_STATS_TABLE_NAME || 'dev-assets-stats-dyn',
  GENERIC_ASSETS_STATS_TABLE_NAME:
    process.env.DYNAMODB_GENERIC_ASSETS_STATS_TABLE_NAME ||
    'dev-generic-assets',
  ROI_MONTHLY_TABLE:
    process.env.DYNAMODB_ROI_MONTHLY_TABLE_NAME || 'dev-roi-monthly-dyn',
  MESSAGE_EVENT_LEADS_TABLE:
    process.env.DYNAMODB_MESSAGE_EVENT_LEADS_TABLE_NAME ||
    'dev-message-event-leads',
  CALL_EVENT_LEADS_TABLE:
    process.env.DYNAMODB_CALL_EVENT_LEADS_TABLE_NAME || 'dev-call-event-leads',
  EMAIL_EVENT_LEADS_TABLE:
    process.env.DYNAMODB_EMAIL_EVENT_LEADS_TABLE_NAME ||
    'dev-email-event-leads',
  UPDATE_BATCH_SIZE: Number(process.env.UPDATE_BATCH_SIZE || '25'),
};
