const { Client } = require('@elastic/elasticsearch');
const config = require('../config');

function createClient() {
  const clientConfig = {
    node: config.elasticsearch.node,
    requestTimeout: config.elasticsearch.requestTimeout,
  };

  if (config.elasticsearch.apiKey) {
    clientConfig.auth = {
      apiKey: config.elasticsearch.apiKey,
    };
  } else if (config.elasticsearch.username && config.elasticsearch.password) {
    clientConfig.auth = {
      username: config.elasticsearch.username,
      password: config.elasticsearch.password,
    };
  }

  return new Client(clientConfig);
}

const client = createClient();

module.exports = client;
