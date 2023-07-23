import datetime
import bson.json_util
import json
import kafka
import kafka.admin
import random
import time

test_producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
test_consumer = kafka.KafkaConsumer(
    'splitter_input',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test_consumer',
)

possible_names = [
    'dog',
    'cat',
    'bird',
    'fish',
    'snake',
]


try:
    admin_client = kafka.admin.KafkaAdminClient(
        bootstrap_servers='localhost:9092',
    )
    if 'splitter_input' in test_consumer.topics():
        admin_client.delete_topics(['splitter_input'])
    counter = 0
    while True:
        counter += 1
        json_message = {
            'timestamp': datetime.datetime.now().isoformat(),
            'value': counter,
            'name': random.choice(possible_names),
            'labels': {
                'test': 'test',
            },
        }
        print('Sending message: ' + str(json_message))
        encoded_message: bytes = json.dumps(
            json_message,
            default=bson.json_util.default,
        ).encode('utf-8')
        test_producer.send('splitter_input', encoded_message)
        time.sleep(5)
except KeyboardInterrupt:
    test_producer.close()
