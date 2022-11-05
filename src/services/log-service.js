const pino = require('pino');
const { v4: uuidV4 } = require('uuid');

const createLogger = ({ cid, ...rest }) =>
  pino({
    base: {
      cid: cid || uuidV4(),
      service: process.env.AWS_LAMBDA_FUNCTION_NAME,
      version: process.env.AWS_LAMBDA_FUNCTION_VERSION,
      ...rest,
    },

    level: process.env.LOG_LEVEL || 'debug',

    transport: process.env.PRETTY_LOG
      ? {
          target: 'pino-pretty',
        }
      : undefined,

    formatters: {
      level: (label) => ({ level: label }),
    },
  });

module.exports = { createLogger };
