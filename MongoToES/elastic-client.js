const { Client } = require('@elastic/elasticsearch');
const config = require('./config');
const readline = require('readline-sync');
const { getCollection } = require('./mongo-client');

const client = new Client({
  node: `http://${config.es_user}:${config.es_pass}@${config.es_host}:${config.es_port}`,
});

const es_type = config.es_type;

async function PushData(collection, index) {
  const cursor = (await getCollection(collection)).find({});
  for await (const doc of cursor) {
    doc.id = doc._id;
    Reflect.deleteProperty(doc, '_id');
    await client.index({
      id: doc.id,
      index: index,
      body: doc,
      type: es_type,
    });
  }
}

async function CheckIndex(collection, index) {
  let exists = false;
  try {
    console.log('Checking Index:', index);
    exists = await client.indices.exists({ index });
  } catch (e) {
    console.log('error', e);
  }

  if (exists) {
    console.log('Index exist!');
  } else {
    console.log('Index not exists! Create new Index (Y/N): ');
    let x = readline.question();
    if (x == 'Y' || x == 'y') {
      client.indices.create({ index });
      //  createCarMapping(client, index, type);
      console.log('Index created! Push data to index? (y/n):');
      x = readline.question();
      if (x == 'Y' || x == 'y') {
        PushData(collection, index);
        console.log('Successfull!');
      }
    } else {
      console.log('Index incorrect! Please change index to sync data.');
      process.exit();
    }
  }
}

module.exports = {
  client: client,
  CheckIndex: CheckIndex,
  PushData: PushData,
};
