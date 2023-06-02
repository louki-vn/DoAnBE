# importing the required modules
from json import loads
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from bson import json_util

# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    'users',
    bootstrap_servers=['192.168.0.2:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

my_client = MongoClient('192.168.0.2:27017')
my_collection = my_client.users.users

for message in my_consumer:
    message = message.value
    # jArray = json.dumps(message, default=json_util.default)
    # my_collection.insert_many(jArray)
    # my_collection.insert_one(message)
    print(message, " added to ", my_collection)
