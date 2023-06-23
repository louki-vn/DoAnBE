const mongodbClient = require("mongodb").MongoClient;
const config = require('./config');

const connectionString = `mongodb://${config.db_host}:${config.db_port}/${config.db_collection}`
let client;

async function getDb() {
  if (!client || !client.isConnected()) {
    client = await mongodbClient.connect(connectionString, { "useNewUrlParser": true, "useUnifiedTopology": true });
    console.log("Connected to Database successfully!!");
  }
  return client.db();
}

async function getCollection(collectionName) {
  const db = await getDb();
  return db.collection(collectionName);
}

module.exports = {
  getCollection
};
