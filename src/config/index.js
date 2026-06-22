require('dotenv').config();

const config = {
  server: {
    port: parseInt(process.env.PORT, 10) || 3000,
  },
  elasticsearch: {
    node: process.env.ES_NODE || 'http://localhost:9200',
    username: process.env.ES_USERNAME || undefined,
    password: process.env.ES_PASSWORD || undefined,
    apiKey: process.env.ES_API_KEY || undefined,
    requestTimeout: 30000,
  },
};

module.exports = config;
