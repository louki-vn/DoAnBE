const result = require('dotenv').config();

module.exports= {
  es_host: process.env.ELASTICSEARCH_HOST,
  es_pass: process.env.ELASTICSEARCH_PASSWORD,
  es_port: process.env.ELASTICSEARCH_PORT,
  es_user:process.env.ELASTICSEARCH_USERNAME,
  es_index:process.env.ELASTICSEARCH_INDEX,
  es_type:process.env.ELASTICSEARCH_TYPE,
  db_host: process.env.MONGO_HOST,
  db_port: process.env.MONGO_PORT,
  db_name: process.env.MONGO_DBNAME,
  db_collection: process.env.MONGO_COLLECTION,
};

if (result.error) {
  console.log(result.error, "[Error Parsing env variables! Missing file .env or something else.]");
  throw result.error;
};

console.log(result.parsed, '[Parsed env variables!]');