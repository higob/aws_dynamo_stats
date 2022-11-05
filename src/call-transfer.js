/* eslint-disable no-async-promise-executor, no-await-in-loop */
const {
  CALL_ANSWERED_BATCH_JOB,
  CALL_DIALED_BATCH_JOB,
} = require('./constants/cache-modules');
const { CALL_ANSWERED, CALL_DIALED } = require('./constants/lead-events');
const { getValue: getSecretValue } = require('./lib/aws/secret');
const CallEventLeadsService = require('./services/dynamodb/call-event-leads-service');
const { createLogger } = require('./services/log-service');
const CachesService = require('./services/mysql/caches-service');
const CallsService = require('./services/mysql/calls-service');
const pool = require('./services/mysql/connector');

/**
 * Return a cache from a last-running batch job
 *
 * @param {string} eventName
 * @param {CachesService} cachesService
 * @returns
 */
const getCacheByEvent = (eventName, cachesService) => {
  const cacheName =
    eventName === CALL_ANSWERED
      ? CALL_ANSWERED_BATCH_JOB
      : CALL_DIALED_BATCH_JOB;

  return cachesService.getCacheByModule(cacheName);
};

/**
 * Preprocess calls data and map the detail which used to insert in dynamodb
 *
 * @param {array} calls
 * @param {string} eventName
 * @returns {array}
 */
const preprocessCalls = (calls, eventName) =>
  calls.reduce((acc, call) => {
    acc.push({
      chiropractor: {
        office_phone: call.business_office_phone,
      },
      lead: {
        id: call.lead_id,
        full_name: call.full_name,
        email: call.email,
      },
      call: {
        sent_to: call.sent_to,
        sent_from: call.sent_from,
        status: call.status,
        direction: call.direction,
        duration: call.duration,
        created: call.created,
        lead_phone: call.lead_phone,
        call_sid: call.call_sid,
      },
      eventName,
    });

    return acc;
  }, []);

/**
 * Return raw calls data by a specific event
 *
 * @param {string} eventName
 * @param {object} cache
 * @param {CallsService} callsService
 *
 * @returns {array}
 */
const getRawCallsByEvent = ({ eventName, cache }, callsService) => {
  if (eventName === CALL_ANSWERED) {
    if (cache) {
      const { created } = JSON.parse(cache.body);

      return callsService.getAnsweredCallsStartFrom(created);
    }

    return callsService.getAnsweredCallsByOldest();
  }

  if (eventName === CALL_DIALED) {
    if (cache) {
      const { created } = JSON.parse(cache.body);

      return callsService.getDialedCallsStartFrom(created);
    }

    return callsService.getDialedCallsByOldest();
  }

  return null;
};

/**
 * Save a cache with a specific module
 *
 * @param {string} eventName
 * @param {object} body
 * @param {CachesService} cachesService
 */
const saveCacheByEvent = async ({ eventName, body }, cachesService) => {
  const cacheName =
    eventName === CALL_ANSWERED
      ? CALL_ANSWERED_BATCH_JOB
      : CALL_DIALED_BATCH_JOB;

  await cachesService.insertUpdateCacheByModule({
    module: cacheName,
    body: JSON.stringify(body),
  });
};

/**
 * Preprocess calls data from mySQL and insert them into dynamodb
 *
 * @param {object} param
 * @param {string} param.eventName
 * @param {object} param.connection
 */
const callBatchJob = async ({ log, eventName, connection }) => {
  const dbsConfigs = await getSecretValue(
    `dbs_configs/${process.env.SLS_STAGE}`
  );

  process.env.DBHOST = dbsConfigs.NHC_DB_HOSTNAME;
  process.env.DBUSER = dbsConfigs.NHC_DB_USERNAME;
  process.env.DBPASSWORD = dbsConfigs.NHC_DB_PASSWORD;
  process.env.DBNAME = dbsConfigs.NHC_DB_DATABASE;

  const connectionPool = connection || pool(process.env);
  const callEventLeadsService = new CallEventLeadsService({ log });
  const cachesService = new CachesService({ log, pool: connectionPool });
  const callsService = new CallsService({ log, pool: connectionPool });

  await cachesService.initConnection();
  await callsService.initConnection();

  /**
   * Insert call data into dynamodb
   *
   * @param {object} call
   */
  const saveCall = (call) =>
    new Promise(async (resolve, reject) => {
      try {
        await callEventLeadsService.insertCallEvent({
          chiropractor: call.chiropractor,
          lead: call.lead,
          call: call.call,
          eventName: call.eventName,
        });

        resolve(true);
      } catch (err) {
        reject(err);
      }
    });

  /**
   * Update call data in batches
   *
   * @param {array} calls
   */
  const batchUpdate = async (calls) => {
    const chunkSize = 50;

    while (calls.length > 0) {
      const batch = calls.splice(0, chunkSize);

      await Promise.all(batch.map(saveCall)).catch((err) =>
        log.error({ err }, '[batchUpdate] Error:')
      );
    }
  };

  log.info({ eventName }, '[callBatchJob] start');

  // Get cache from last-running job
  const callCache = await getCacheByEvent(eventName, cachesService);

  log.info({ callCache }, '[callBatchJob]');

  // Get raw call data from mySQL
  const rawCalls = await getRawCallsByEvent(
    { eventName, cache: callCache },
    callsService
  );

  // Terminate the job if nothing to be added
  if (!rawCalls) {
    log.info({ status: 'none added' }, '[callBatchJob]');

    await cachesService.terminate();
    await callsService.terminate();
    connectionPool.end();

    return;
  }

  const processedCalls = preprocessCalls(rawCalls, eventName);

  log.info({ message: 'saving calls into dynamodb' }, '[callBatchJob]');

  // Get the last item of calls array
  const lastCall = processedCalls.slice(-1)[0];

  await batchUpdate(processedCalls);

  log.info({ status: 'calling saveCacheByEvent' }, '[callBatchJob]');

  // Update cache
  await saveCacheByEvent(
    {
      eventName,
      body: {
        call_sid: lastCall.call.call_sid,
        created: lastCall.call.created,
      },
    },
    cachesService
  );

  log.info({ status: 'completed' }, '[callBatchJob]');

  // Release the connections
  await cachesService.terminate();
  await callsService.terminate();

  // Repeat the job
  await callBatchJob({ log, eventName, connection: connectionPool });
};

module.exports.transferDialedCallsHandler = async () => {
  const log = createLogger({ handler: 'transfer-dialed-call-handler' });

  await callBatchJob({ log, eventName: CALL_DIALED });
};

module.exports.transferAnsweredCallsHandler = async () => {
  const log = createLogger({ handler: 'transfer-answered-call-handler' });

  await callBatchJob({ log, eventName: CALL_ANSWERED });
};
