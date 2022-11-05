const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

const smClient = new SecretsManagerClient();

const getValue = async (secretName) => {
  try {
    const params = {
      SecretId: secretName,
    };
    const data = await smClient.send(new GetSecretValueCommand(params));

    let decodedBinarySecret;
    if (data) {
      if ('SecretString' in data) {
        decodedBinarySecret = JSON.parse(data.SecretString);
      } else {
        const buff = Buffer.alloc(data.SecretBinary, 'base64');
        decodedBinarySecret = buff.toString('ascii');
      }
    }
    return decodedBinarySecret;
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('ERROR', err);
    throw new Error(`Error retrieving ${secretName} value`);
  }
};

module.exports = {
  getValue,
};
