/* eslint-disable import/no-extraneous-dependencies,no-unused-vars */
require('dotenv').config();

const { handler } = require('../src/function');

const eventEventAdCreativeChanged = require('./events/ad-creative-changed.json');
const eventChiropractorAvatarChanged = require('./events/chiropractor-profile-changed.json');

(async () => {
  await handler({ Records: [{ messageAttributes: {} }] });
})();
