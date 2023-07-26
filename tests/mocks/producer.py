import datetime
import bson.json_util
import json
import kafka
import kafka.admin
import random
import time

test_producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
test_consumer = kafka.KafkaConsumer(
    'animals',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test_consumer',
)

POSSIBLE_KEYS = [
    'dog',
    'cat',
    'bird',
    'fish',
    'snake',
]

TEST_TOPIC = 'animals'


try:
    admin_client = kafka.admin.KafkaAdminClient(
        bootstrap_servers='localhost:9092',
    )
    if TEST_TOPIC in test_consumer.topics():
        admin_client.delete_topics([TEST_TOPIC])
    counter = 0
    while True:
        counter += 1
        json_message = {
            'timestamp': datetime.datetime.now().isoformat(),
            'value': counter,
            'name': random.choice(POSSIBLE_KEYS),
            'labels': {
                'test': 'test',
            },
        }
        print(f'Sending message: {json_message}')
        encoded_message: bytes = json.dumps(
            json_message,
            default=bson.json_util.default,
        ).encode('utf-8')
        test_producer.send(TEST_TOPIC, encoded_message)
        time.sleep(5)
except KeyboardInterrupt:
    test_producer.close()
