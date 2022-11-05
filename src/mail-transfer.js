/* eslint-disable no-async-promise-executor, no-await-in-loop */
const {
  EMAIL_OPENED_BATCH_INSERT_JOB,
  EMAIL_DELIVERED_BATCH_INSERT_JOB,
  EMAIL_LINK_CLICKED_BATCH_INSERT_JOB,
  EMAIL_SPAMMED_BATCH_INSERT_JOB,
  EMAIL_UNSUBSCRIBED_BATCH_INSERT_JOB,
  EMAIL_GROUP_UNSUBSCRIBED_BATCH_INSERT_JOB,
} = require('./constants/cache-modules');
const {
  EMAIL_OPENED,
  EMAIL_DELIVERED,
  EMAIL_LINK_CLICKED,
  EMAIL_MARKED_AS_SPAM,
  EMAIL_UNSUBSCRIBED,
  EMAIL_GROUP_UNSUBSCRIBED,
} = require('./constants/lead-events');
const { getValue: getSecretValue } = require('./lib/aws/secret');
const MailEventLeadsService = require('./services/dynamodb/mail-event-leads-service');
const { createLogger } = require('./services/log-service');
const CachesService = require('./services/mysql/caches-service');
const pool = require('./services/mysql/connector');
const LeadsService = require('./services/mysql/leads-service');
const SendGridService = require('./services/redshift/sendgrid-service');

// Return jobs name by event name
const MAIL_EVENT = {
  [EMAIL_OPENED]: EMAIL_OPENED_BATCH_INSERT_JOB,
  [EMAIL_DELIVERED]: EMAIL_DELIVERED_BATCH_INSERT_JOB,
  [EMAIL_LINK_CLICKED]: EMAIL_LINK_CLICKED_BATCH_INSERT_JOB,
  [EMAIL_MARKED_AS_SPAM]: EMAIL_SPAMMED_BATCH_INSERT_JOB,
  [EMAIL_UNSUBSCRIBED]: EMAIL_UNSUBSCRIBED_BATCH_INSERT_JOB,
  [EMAIL_GROUP_UNSUBSCRIBED]: EMAIL_GROUP_UNSUBSCRIBED_BATCH_INSERT_JOB,
};

/**
 * Save last insert detail in mySQL cache table
 *
 * @param {object} event
 * @param {string} event.eventName
 * @param {object} event.body
 * @param {CachesService} cacheService
 *
 * @returns {void}
 */
const saveCacheByEvent = ({ eventName, body }, cacheService) => {
  const cacheName = MAIL_EVENT[eventName];

  return cacheService.insertUpdateCacheByModule({
    module: cacheName,
    body: JSON.stringify(body),
  });
};

/**
 * Return mails data from redshift
 *
 * @param {object} event
 * @param {string} event.eventName
 * @param {object} event.cache
 * @param {SendGridService} sendgridService
 *
 * @returns {array}
 */
const getRawMailsByEvent = async ({ eventName, cache }, sendgridService) => {
  const receivedAt = cache ? JSON.parse(cache.body).received_at : null;

  if (eventName === EMAIL_OPENED) {
    return receivedAt
      ? sendgridService.getOpenedEmailsStartFrom(receivedAt)
      : sendgridService.getOpenedEmailsByOldest();
  }

  if (eventName === EMAIL_DELIVERED) {
    return receivedAt
      ? sendgridService.getDeliveredEmailsStartFrom(receivedAt)
      : sendgridService.getDeliveredEmailsByOldest();
  }

  if (eventName === EMAIL_LINK_CLICKED) {
    return receivedAt
      ? sendgridService.getClickedEmailsStartFrom(receivedAt)
      : sendgridService.getClickedEmailsByOldest();
  }

  if (eventName === EMAIL_MARKED_AS_SPAM) {
    return receivedAt
      ? sendgridService.getSpamReportedEmailsStartFrom(receivedAt)
      : sendgridService.getSpamReportedEmailsByOldest();
  }

  if (eventName === EMAIL_UNSUBSCRIBED) {
    return receivedAt
      ? sendgridService.getUnsubscribedEmailsStartFrom(receivedAt)
      : sendgridService.getUnsubscribedEmailsByOldest();
  }

  if (eventName === EMAIL_GROUP_UNSUBSCRIBED) {
    return receivedAt
      ? sendgridService.getGroupUnsubscribedEmailsStartFrom(receivedAt)
      : sendgridService.getGroupUnsubscribedEmailsByOldest();
  }
  return null;
};

/**
 * Return cache by event type
 *
 * @param {string} eventName
 * @param {CachesService} cacheService
 *
 * @returns {object}
 */
const getCacheByEvent = (eventName, cacheService) => {
  const cacheName = MAIL_EVENT[eventName];

  return cacheService.getCacheByModule(cacheName);
};

/**
 * Preprocess mail data for dynamodb fields
 *
 * @param {object} mails
 * @param {string} eventName
 * @returns {array}
 */
const preprocessMails = (mails, eventName) =>
  mails.reduce((acc, mail) => {
    acc.push({
      mail: {
        id: mail.id,
        email: mail.email,
        received_at: mail.received_at,
      },
      eventName,
    });

    return acc;
  }, []);

/**
 * Preprocess raw mail data from redshift and insert to dynamodb
 *
 * @param {object} param
 * @param {string} param.eventName
 * @param {object} param.connection
 * @returns {void}
 */
const mailBatchJob = async ({ log, eventName, connection }) => {
  const redshiftConfigs = await getSecretValue(`Redshift`);

  process.env.RS_HOST = redshiftConfigs.host;
  process.env.RS_USERNAME = redshiftConfigs.username;
  process.env.RS_PASSWORD = redshiftConfigs.password;

  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );

  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const connectionPool = connection || pool(process.env);
  const sendgridService = new SendGridService({ log });
  const cacheService = new CachesService({ log, pool: connectionPool });
  const mailEventLeadsService = new MailEventLeadsService({ log });
  const leadsService = new LeadsService({ log, pool: connectionPool });

  await cacheService.initConnection();
  await leadsService.initConnection();

  log.info({ eventName }, '[mailBatchJob] start');

  const emailCache = await getCacheByEvent(eventName, cacheService);

  const saveMail = ({ mail, evName }) =>
    new Promise(async (resolve, reject) => {
      try {
        const lead = await leadsService.getLeadByEmail(mail.email);

        await mailEventLeadsService.insertEmailEvent({
          mail,
          evName,
          chiropractor: {
            office_phone: lead ? lead.office_phone : null,
          },
          lead: {
            full_name: lead ? lead.full_name : null,
            id: lead ? lead.id : null,
          },
        });

        resolve(true);
      } catch (err) {
        log.error({ err }, '[saveMail] Error');
        reject(err);
      }
    });

  const batchUpdate = async (mails) => {
    const chunkSize = 50;

    while (mails.length > 0) {
      const batch = mails.splice(0, chunkSize);

      await Promise.all(batch.map(saveMail)).catch((err) =>
        log.error({ err }, '[batchUpdate] Error')
      );
    }
  };

  log.info({ emailCache }, '[mailBatchJob]');

  const rawMails = await getRawMailsByEvent(
    { eventName, cache: emailCache },
    sendgridService
  );

  log.info({ total: rawMails.length }, '[mailBatchJob]');

  // Terminate this job if there is no more raw mails data
  if (!rawMails.length) {
    log.info({ status: 'none added' }, '[mailBatchJob]');

    await cacheService.terminate();
    await leadsService.terminate();
    connectionPool.end();

    return;
  }

  const processedMails = preprocessMails(rawMails, eventName);

  log.info({ message: 'saving mails into dynamodb' }, '[mailBatchJob]');

  const lastMail = processedMails.slice(-1)[0];

  await batchUpdate(processedMails);

  log.info({ status: 'calling saveCacheByEvent' }, '[mailBatchJob]');

  // Save the cache with the last insert mail detail
  await saveCacheByEvent(
    {
      eventName,
      body: {
        id: lastMail.mail.id,
        received_at: lastMail.mail.received_at,
      },
    },
    cacheService
  );

  log.info({ status: 'completed' }, '[mailBatchJob]');

  // Release the connections
  await cacheService.terminate();
  await leadsService.terminate();

  await mailBatchJob({ log, eventName, connection: connectionPool });
};

module.exports.transferOpenedMailsHandler = async () => {
  const log = createLogger({ handler: 'transfer-opened-mails-handler' });

  await mailBatchJob({ log, eventName: EMAIL_OPENED });
};

module.exports.transferClickedMailsHandler = async () => {
  const log = createLogger({ handler: 'transfer-opened-mails-handler' });

  await mailBatchJob({ log, eventName: EMAIL_LINK_CLICKED });
};

module.exports.transferDeliveredMailsHandler = async () => {
  const log = createLogger({ handler: 'transfer-opened-mails-handler' });

  await mailBatchJob({ log, eventName: EMAIL_DELIVERED });
};

module.exports.transferSpammedReportMailsHandler = async () => {
  const log = createLogger({ handler: 'transfer-opened-mails-handler' });

  await mailBatchJob({ log, eventName: EMAIL_MARKED_AS_SPAM });
};

module.exports.transferUnsubscribedMailsHandler = async () => {
  const log = createLogger({ handler: 'transfer-opened-mails-handler' });

  await mailBatchJob({ log, eventName: EMAIL_UNSUBSCRIBED });
  await mailBatchJob({ log, eventName: EMAIL_GROUP_UNSUBSCRIBED });
};
