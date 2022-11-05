const mysql = require('mysql');

/**
 * Return a pool of mySQL connection
 *
 * @param {object} setting
 * @returns
 */
const pool = (setting) =>
  mysql.createPool({
    connectionLimit: 3,
    host: setting.DBHOST,
    user: setting.DBUSER,
    password: setting.DBPASSWORD,
    database: setting.DBNAME,
    timezone: 'UTC',
  });

module.exports = pool;
