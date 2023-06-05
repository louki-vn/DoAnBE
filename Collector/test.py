from configparser import ConfigParser
from kafka import KafkaProducer
from json import dumps


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
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'], value_serializer=lambda x: dumps(x).encode('utf-8'))
print(kafka_config['enable_auto_commit'])
