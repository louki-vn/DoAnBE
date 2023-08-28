from configparser import ConfigParser
from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads


def read_config():
    data = {}
    config = ConfigParser()
    config.read('../kafka_config.ini')
    data['topic_name'] = config.get('kafka', 'topic_name')
    data['bootstrap_servers'] = config.get('kafka', 'bootstrap_servers')
    data['group_id'] = config.get('kafka', 'group_id')
    data['auto_offset_reset'] = config.get('kafka', 'auto_offset_reset')
    data['enable_auto_commit'] = config.get('kafka', 'enable_auto_commit')

    return data


kafka_config = read_config()
# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    kafka_config['topic_name'],
    bootstrap_servers=kafka_config['bootstrap_servers'],
    auto_offset_reset=kafka_config['auto_offset_reset'],
    enable_auto_commit=kafka_config['enable_auto_commit'],
    group_id=kafka_config['group_id'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

connectString = 'mongodb://10.8.0.3:27021/?directConnection=true'


try:
    my_client = MongoClient(connectString)
    my_collection = my_client.test.test
    print("Connected successfully!")
except:
    print("Could not connect to MongoDB")


for message in my_consumer:
    message = message.value
    try:
        my_collection.insert_one(message)
        print("Data inserted successfull!")
    except:
        print("Could not insert into database!")
