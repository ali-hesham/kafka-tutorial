from time import sleep
from json import dumps
from kafka import KafkaProducer

# initialize kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# generate numbers from 0 to 999 and send it to broker in a topic test_topic
for e in range(1000):
    data = {'number': e}
    producer.send('test_topic', value=data)
    sleep(5)  # sleep for 5 seconds to make sure message delivered
