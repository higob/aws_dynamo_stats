const moment = require('moment');

const getUnixTimestamp = (date) => moment.utc(date).unix();

const formatUTCDate = (date) => moment.utc(date).format();

const getDateISO8601 = (date = null) =>
  date ? moment().utc(date).format() : moment().utc().format();

module.exports = {
  getUnixTimestamp,
  formatUTCDate,
  getDateISO8601,
};
