from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads

topic = 'users'
# generating the Kafka Consumer
my_consumer = KafkaConsumer(
    topics=topic,
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
    try:
        my_collection.insert_one(message)
        print("Data inserted successfull!")
    except:
        print("Could not insert into database!")
