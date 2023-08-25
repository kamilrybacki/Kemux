import logging

import kafka
import pytest

import conftest
import helpers

import lib.producer.start


@pytest.mark.order(5)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_existence_of_new_topic(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str) -> None:
    producer_consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)

    new_topic_consumer: kafka.KafkaConsumer = use_consumer(topic)
    assert new_topic_consumer.bootstrap_connected()
    tests_logger.info(f'Connected to {topic} successfully')

    splitted_message_found = False
    while not splitted_message_found:
        produced_message = next(producer_consumer)
        produced_json = produced_message.value.decode('utf-8')
        if '__faust' in produced_json:
            tests_logger.info('Skipping Faust topic init message')
            continue

        split_message = next(new_topic_consumer)
        split_json = split_message.value.decode('utf-8')

        tests_logger.info(f'Produced JSON: {produced_json}')
        tests_logger.info(f'Split JSON: {split_json}')
    splitted_message_found = True
