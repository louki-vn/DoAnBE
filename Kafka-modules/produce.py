# importing the required libraries
from time import sleep
from json import dumps
from kafka import KafkaProducer

my_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# generating the numbers ranging from 1 to 500
# for n in range(500):
#     my_data = {'num': n}
#     my_producer.send('testnum', value=my_data)
#     sleep(5)
for n in range(1):
    my_data = {'firstName': 'heiu', 'lastName': 'Hiue'}
    my_producer.send('users', value=my_data)
    sleep(3)
