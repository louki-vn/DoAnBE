const { Client } = require('@elastic/elasticsearch')
const config = require('./config');
const readline = require("readline-sync");
  


const client = new Client({
    node: `http://${config.es_user}:${config.es_pass}@${config.es_host}:${config.es_port}`,
});

async function CheckIndex(index) {
    let exists = false;
    try {
      console.log('Checking Index:', index);
      const existsResponse = await client.indices.exists({ index });
      exists = existsResponse.body;
    } catch (e) {
      console.log('error', e);
    }
  
    if (exists) {
      console.log('Index exist!');
      return;
    }
    else {
      console.log('Index not exists! Create new Index (Y/N): ')
      let x = readline.question();
      if (x == 'Y' || x == 'y'){
        client.indices.create({ index });
        //  createCarMapping(client, index, type);
        console.log('Index created!');
      }
      else {
        console.log('Index incorrect! Please change index to sync data.')
        process.exit()
      }    
    }
  }

module.exports = {
    client: client,
    CheckIndex: CheckIndex
};