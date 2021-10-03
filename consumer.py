from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',  # consumer starts reading at the latest committed offset
    enable_auto_commit=True,  # auto_commit messages when received
    group_id='my-group',  # add consumer to a group to make auto commit
    value_deserializer=lambda x: loads(x.decode('utf-8')))  # message deserialization
