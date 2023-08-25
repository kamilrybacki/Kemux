import ast
import time
import logging
import typing

import kafka
import pytest

import conftest
import helpers

import lib.producer.start


FILTERING_TIMEOUT = 10
NUMBER_OF_PRODUCED_MESSAGES_SAMPLES = 100


@pytest.mark.order(5)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_existence_of_new_topic(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str) -> None:
    new_topic_consumer: kafka.KafkaConsumer = use_consumer(topic)
    assert new_topic_consumer.bootstrap_connected()
    tests_logger.info(f'Connected to {topic} successfully')


@pytest.mark.order(6)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_message_filtering(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str):
    producer_consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)
    tests_logger.info(f'Connected to {lib.producer.start.TEST_TOPIC} successfully')

    outputs_class_name_for_topic = topic.title().replace('-', '')
    filtering_function = helpers.get_filtering_function_for_topic(outputs_class_name_for_topic)

    assert filtering_function.__annotations__.get('message')
    tests_logger.info(f'Filtering function for {topic} found')

    filtering_start_time = time.time()
    produced_message = next(producer_consumer)
    produced_json = ast.literal_eval(
        produced_message.value.decode('utf-8')
    )
    while not filtering_function(produced_json):
        produced_message = next(producer_consumer)
        produced_json = ast.literal_eval(
            produced_message.value.decode('utf-8')
        )
        if '__faust' in produced_json:
            continue
        if time.time() - filtering_start_time > FILTERING_TIMEOUT:
            raise TimeoutError(f'Filtering function for {topic} timed out (timeout: {FILTERING_TIMEOUT})')
    tests_logger.info(f'Filtering function for {topic} works as expected')


@pytest.mark.order(7)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_message_splitting(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str):
    producer_consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)

    produced_messages_names: list[str] = []
    while True:
        produced_message = next(producer_consumer)
        produced_json = ast.literal_eval(
            produced_message.value.decode('utf-8')
        )
        if '__faust' not in produced_json:
            produced_messages_names.append(
                produced_json.get('name')
            )
        if len(produced_messages_names) == NUMBER_OF_PRODUCED_MESSAGES_SAMPLES and set(produced_messages_names) == {*lib.producer.start.POSSIBLE_KEYS}:
            break
    tests_logger.info(f'Produced messages: {produced_messages_names}')

    outputs_class_name_for_topic = topic.title().replace('-', '')
    filtering_function = helpers.get_filtering_function_for_topic(outputs_class_name_for_topic)

    manually_filtered_messages_names: list[str] = [
        produced_message_name
        for produced_message_name in produced_messages_names
        if filtering_function(
            {
                'name': produced_message_name,
            }
        )
    ]
    tests_logger.info(f'Manually filtered messages: {manually_filtered_messages_names}')

    new_topic_consumer: kafka.KafkaConsumer = use_consumer(topic)
    assert new_topic_consumer.bootstrap_connected()
    tests_logger.info(f'Connected to {topic} successfully')

    expected_number_of_messages = len(manually_filtered_messages_names)

    new_topic_messages_names: list[str] = [
        ast.literal_eval(
            next(new_topic_consumer).value.decode('utf-8')
        ).get('name')
        for _ in range(expected_number_of_messages)
    ]

    assert new_topic_messages_names == manually_filtered_messages_names
    tests_logger.info('Splitting works as expected')
