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
        .must(
          esb.matchQuery(
            'Category Name',
            'Adware Backdoor Behavior BrowserModifier Constructor DDoS Exploit HackTool Joke Misleading MonitoringTool Program Personal Web Server (PWS) Ransom RemoteAccess Rogue SettingsModifier SoftwareBundler Spammer Spoofer Spyware Tool Trojan TrojanClicker TrojanDownloader TrojanNotifier TrojanProxy TrojanSpy VirTool Virus Worm Unwanted software'
          )
        )
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
    } else if (
      change.fullDocument.EventID == 5001 ||
      change.fullDocument.EventID == 1006 ||
      change.fullDocument.EventID == 1116 ||
      change.fullDocument.EventID == 4649 ||
      change.fullDocument.EventID == 5010 ||
      change.fullDocument.EventID == 5012
    ) {
      await catchData(change, change);
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
