from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads
import parse_XML_log
import sys

sys.path.insert(0, '/home/louki/Downloads/DoAn/Collector')


# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    'users',
    bootstrap_servers=['192.168.0.2:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)


try:
    my_client = MongoClient('127.0.0.1', 27017)
    my_collection = my_client.users.users
    print("Connected successfully!")
except:
    print("Could not connect to MongoDB")


for message in my_consumer:
    message = message.value
    log = parse_XML_log(message)
    try:
        my_collection.insert_one(log)
        print("Data inserted successfull!")
    except:
        print("Could not insert into MongoDB!")
