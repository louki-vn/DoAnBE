const {
  getUpsertChangeStream,
  getDeleteChangeStream,
} = require('./change-identifier');
const { saveResumeTaken } = require('./token-provider');
const { client, CheckIndex } = require('./elastic-client');
const config = require('./config');
const esb = require('elastic-builder'); //the builder
const fs = require('fs');

const index = config.es_index;
const type = config.es_type;
const collection = config.db_collection;

const listID = [
  5001, 1006, 1116, 4649, 5010, 5012, 4618, 4649, 4719, 4765, 4766, 4794, 4897,
  4964, 5124, 4720,
];

const keywords = [
  'Adware',
  'Backdoor',
  'Behavior',
  'Browser Modifier',
  'Constructor',
  'DDoS',
  'Exploit',
  'Hack Tool',
  'Joke',
  'Misleading',
  'Monitoring Tool',
  'Program',
  'Personal Web Server (PWS)',
  'Ransom',
  'Remote Access',
  'Rogue',
  'Settings Modifier',
  'Software Bundler',
  'Spammer',
  'Spoofer',
  'Spyware',
  'Tool',
  'Trojan',
  'Trojan Clicker',
  'Trojan Downloader',
  'Trojan Notifier',
  'Trojan Proxy',
  'Trojan Spy',
  'Vir Tool',
  'Virus',
  'Worm',
  'Potentially Unwanted Software',
];

async function writeToFile(content) {
  try {
    const values = Object.values(content).join(' ') + '\n';

    await fs.appendFile(
      'D:/NeuralLog-main/data/raw/testcopy.log',
      values,
      () => {}
    );
  } catch (err) {
    console.log(err);
  }
}

async function search(id) {
  const requestBody = esb
    .requestBodySearch()
    .query(
      esb
        .boolQuery()
        .must(esb.termQuery('id', id))
        .must(esb.termsQuery('Category Name', keywords))
    );
  return client.search({ index: index, body: requestBody.toJSON() });
}

async function catchData(data, change) {
  await client.index({
    id: change.fullDocument.id,
    index: 'anomaly',
    body: change.fullDocument,
    type: type,
  });
  const jsonData = JSON.stringify(data);
  fs.writeFile('./test.json', jsonData, (err) => {
    if (err) {
      console.error(err);
    }
  });
}

CheckIndex(collection, index);

(async () => {
  const upsertChangeStream = await getUpsertChangeStream(collection);
  upsertChangeStream.on('change', async (change) => {
    console.log(
      'Pushing data to elasticsearch with id',
      change.fullDocument._id
    );
    change.fullDocument.id = change.fullDocument._id;
    Reflect.deleteProperty(change.fullDocument, '_id');
    const response = await client.index({
      id: change.fullDocument.id,
      index: index,
      body: change.fullDocument,
      type: type,
    });
    writeToFile(change.fullDocument);

    if (change.fullDocument['Category Name'] != null) {
      let idString = change.fullDocument.id.toString();
      const result = await search(idString);
      const data = result.hits.hits.map((log) => {
        return {
          score: log._score,
          id: log._id,
        };
      });
      if (data != null) {
        await catchData(data, change);
      }
    } else {
      listID.map(async (element, index) => {
        if (element === change.fullDocument.EventID) {
          await catchData(change, change);
        }
      });
      // await catchData(change, change);
    }
    console.log('document ', response.result);
    await saveResumeTaken(change._id, 'SOME_UPSERT_TOKEN_ID');
  });

  upsertChangeStream.on('error', (error) => {
    console.error(error);
  });

  const deleteChangeStream = await getDeleteChangeStream(collection);
  deleteChangeStream.on('change', async (change) => {
    console.log(
      'Deleting data from elasticsearch with id',
      change.documentKey._id
    );
    const response = await client.delete({
      id: change.documentKey._id,
      index: index,
      type: type,
    });
    // console.log(response)
    console.log('document ', response.result);
    await saveResumeTaken(change._id, 'SOME_DELETE_TOKEN_ID');
  });

  deleteChangeStream.on('error', (error) => {
    console.error(error);
  });
})();
