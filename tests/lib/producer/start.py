import datetime
import json
import logging
import random
import os
import time

import bson.json_util
import kafka
import kafka.admin

POSSIBLE_KEYS = [
    'dog',
    'cat',
    'bird',
    'spider',
    'fish',
    'snake',
    'bat',
    'lizard',
]

TEST_TOPIC = 'animals'


def start_producer() -> None:
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        level=logging.INFO,
    )
    producer_logger = logging.getLogger('PRODUCER')
    if not (kafka_host := os.environ.get('KEMUX_KAFKA_ADDRESS')):
        raise ValueError('KEMUX_KAFKA_ADDRESS not set')
    test_producer = kafka.KafkaProducer(
        bootstrap_servers=kafka_host,
    )
    test_consumer = kafka.KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=kafka_host,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test_consumer',
    )

    try:
        admin_client = kafka.admin.KafkaAdminClient(
            bootstrap_servers=kafka_host,
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
            producer_logger.info(f'Sending message: {json_message}')
            encoded_message: bytes = json.dumps(
                json_message,
                default=bson.json_util.default,
            ).encode('utf-8')
            test_producer.send(TEST_TOPIC, encoded_message)
            time.sleep(0.25)
    except KeyboardInterrupt:
        test_producer.close()


if __name__ == '__main__':
    start_producer()
