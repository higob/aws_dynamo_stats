/* eslint-disable no-async-promise-executor, no-await-in-loop */
const {
  MESSAGE_SENT_BATCH_JOB,
  MESSAGE_RECEIVED_BATCH_JOB,
} = require('./constants/cache-modules');
const { MESSAGE_SENT, MESSAGE_RECEIVED } = require('./constants/lead-events');
const { getValue: getSecretValue } = require('./lib/aws/secret');
const MessageEventLeadsService = require('./services/dynamodb/message-event-leads-service');
const { createLogger } = require('./services/log-service');
const CachesService = require('./services/mysql/caches-service');
const pool = require('./services/mysql/connector');
const MessagesService = require('./services/mysql/messages-service');

/**
 * Map properties of chiropractor, lead, message, eventName from messages
 *
 * @param {array} messages
 * @param {string} eventName
 * @returns {array}
 */
const preprocessMessages = (messages, eventName) =>
  messages.reduce((acc, message) => {
    acc.push({
      chiropractor: {
        office_phone: message.business_office_phone,
      },
      lead: {
        id: message.lead_id,
        full_name: message.full_name,
        email: message.email,
      },
      message: {
        sent_to: message.sent_to,
        sent_from: message.sent_from,
        status: message.status,
        direction: message.direction,
        body: message.body,
        created: message.created,
        lead_phone: message.lead_phone,
        message_sid: message.message_sid,
      },
      eventName,
    });

    return acc;
  }, []);

/**
 * Return cache by event type
 *
 * @param {string} eventName
 * @param {CachesService} cachesService
 * @returns {object}
 */
const getCacheByEvent = (eventName, cachesService) => {
  const cacheName =
    eventName === MESSAGE_SENT
      ? MESSAGE_SENT_BATCH_JOB
      : MESSAGE_RECEIVED_BATCH_JOB;

  return cachesService.getCacheByModule(cacheName);
};

/**
 * Insert or update a cache by event type
 *
 * @param {string} eventName
 * @param {object} body
 * @param {CachesService} cachesService
 * @returns {object}
 */
const saveCacheByEvent = ({ eventName, body }, cachesService) => {
  const cacheName =
    eventName === MESSAGE_SENT
      ? MESSAGE_SENT_BATCH_JOB
      : MESSAGE_RECEIVED_BATCH_JOB;

  return cachesService.insertUpdateCacheByModule({
    module: cacheName,
    body: JSON.stringify(body),
  });
};

const getRawMessagesByEvent = ({ eventName, cache }, messagesService) => {
  if (eventName === MESSAGE_SENT) {
    // Get the sent messages from where it has stopped last time
    if (cache) {
      const { created } = JSON.parse(cache.body);

      return messagesService.getSentMessagesStartFrom(created);
    }

    return messagesService.getSentMessagesByOldest();
  }

  if (eventName === MESSAGE_RECEIVED) {
    // Get the received messages from where it has stopped last time
    if (cache) {
      const { created } = JSON.parse(cache.body);

      return messagesService.getReceivedMessagesStartFrom(created);
    }

    return messagesService.getReceivedMessagesByOldest();
  }

  return null;
};

/**
 * Preprocess raw messages from mySQL database
 * and insert into dynamodb
 *
 * @param {object} param
 * @param {string} param.eventName
 * @param {object} param.connection
 * @returns
 */
const messageBatchJob = async ({ log, eventName, connection }) => {
  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );

  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const connectionPool = connection || pool(process.env);

  const messageEventLeadsService = new MessageEventLeadsService({ log });
  const cachesService = new CachesService({ log, pool: connectionPool });
  const messagesService = new MessagesService({ log, pool: connectionPool });

  await cachesService.initConnection();
  await messagesService.initConnection();

  log.info({ eventName }, '[messageBatchJob] start');

  const messageCache = await getCacheByEvent(eventName, cachesService);

  const saveMessage = (message) =>
    new Promise(async (resolve, reject) => {
      try {
        await messageEventLeadsService.insertMessageEvent({
          chiropractor: message.chiropractor,
          lead: message.lead,
          message: message.message,
          eventName: message.eventName,
        });

        resolve(true);
      } catch (err) {
        reject(err);
      }
    });

  /**
   * Update messages in batch
   *
   * @param {array} messages
   */
  const batchUpdate = async (messages) => {
    const chunkSize = 50;

    while (messages.length > 0) {
      const batch = messages.splice(0, chunkSize);

      await Promise.all(batch.map(saveMessage)).catch((err) =>
        log.error({ err }, '[batchUpdate] Error:')
      );
    }
  };

  log.info({ messageCache }, '[messageBatchJob]');

  // Get raw messages from mySQL database
  const rawMessages = await getRawMessagesByEvent(
    { eventName, cache: messageCache },
    messagesService
  );

  // End the process if no more message to transfer
  if (!rawMessages) {
    log.info({ status: 'none added' }, '[messageBatchJob]');

    await cachesService.terminate();
    await messagesService.terminate();
    connectionPool.end();

    return;
  }

  // Map dynamodb required details from raw messages
  const processedMessages = preprocessMessages(rawMessages, eventName);

  log.info({ message: 'saving messages into dynamodb' }, '[messageBatchJob]');

  const lastMessage = processedMessages.slice(-1)[0];

  // Mass transfer to dynamodb
  await batchUpdate(processedMessages);

  log.info({ status: 'calling saveCacheByEvent' }, '[messageBatchJob]');

  // Save the last inserted message into cache for resumability
  await saveCacheByEvent(
    {
      eventName,
      body: {
        message_sid: lastMessage.message.message_sid,
        created: lastMessage.message.created,
      },
    },
    cachesService
  );

  log.info({ status: 'completed' }, '[messageBatchJob]');

  // Release the conenctions
  await cachesService.terminate();
  await messagesService.terminate();

  await messageBatchJob({ eventName, connection: connectionPool });
};

module.exports.transferSentMessagesHandler = async () => {
  const log = createLogger({ handler: 'transfer-sent-message-handler' });

  await messageBatchJob({ log, eventName: MESSAGE_SENT });
};

module.exports.transferReceivedMessagesHandler = async () => {
  const log = createLogger({ handler: 'transfer-received-message-handler' });

  await messageBatchJob({ log, eventName: MESSAGE_RECEIVED });
};
