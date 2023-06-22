const { getCollection } = require("./mongo-client");
const { getResumetoken } = require("./token-provider");
const config = require('./config');


const collection = config.db_collection;

async function getUpsertChangeStream() {
  console.log("@@@@@@@@@@@@@@@@@@@@");
  const resumeToken = await getResumetoken("SOME_UPSERT_TOKEN_ID");
  console.log("resumeToken", resumeToken);
  const changeStream = (await getCollection(collection)).watch([
    {
      "$match": {
        "operationType": {
          "$in": ["insert", "update", "replace"]
        }
      }
    },
    {
      "$project": {
        "documentKey": false
      }
    }
  ], { "resumeAfter": resumeToken, "fullDocument": "updateLookup" });

  return changeStream;
}

async function getDeleteChangeStream() {
  console.log("@@@@@@@@@@@@@@@@@@@@");
  const resumeToken = await getResumetoken("SOME_DELETE_TOKEN_ID");
  console.log("resumeToken", resumeToken);
  const changeStream = (await getCollection(collection)).watch([
    {
      "$match": {
        "operationType": {
          "$in": ["delete"]
        }
      }
    },
    {
      "$project": {
        "documentKey": true
      }
    }
  ], { "resumeAfter": resumeToken });

  return changeStream;
}

module.exports = {
  getUpsertChangeStream,
  getDeleteChangeStream
};

