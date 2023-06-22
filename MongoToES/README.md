### Prerequisites

- Install Mongodb 3.6 or more in replica set mode.
- Install Elasticsearch listening on `http://localhost:9200`.

## How to install?

`npm install`

### How to run?

`node mongo-to-elasticsearch.js`

This command will open the change stream and push all the insert, updates and deletes to elasticsearch in real time.
