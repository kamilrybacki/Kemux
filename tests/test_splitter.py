# sourcery skip: no-conditionals-in-tests,no-loop-in-tests
import ast
import time
import logging

import kafka
import pytest

import conftest
import helpers

import lib.producer.start
import kemux.data.stream

FILTERING_TIMEOUT = 10
NUMBER_OF_PRODUCED_MESSAGES_SAMPLES = 100
TEST_STREAMS_INFO: list[tuple] = [
    ('2', ['5', '6', '7']),
    ('3', ['5', '6', '7']),
    ('1', ['2', '3', '4']),
    ('7', ['8', '9', '10']),
]
EXPECTED_STREAMS_ORDER = [
    ('1', ['2', '3', '4']),
    ('3', ['5', '6', '7']),
    ('2', ['5', '6', '7']),
    ('7', ['8', '9', '10']),
]


@pytest.mark.order(4)
def test_streams_ordering(tests_logger: logging.Logger):
    streams_info = TEST_STREAMS_INFO.copy()
    streams_info = kemux.data.stream.find_streams_order(streams_info)
    assert streams_info == EXPECTED_STREAMS_ORDER
    tests_logger.info('Streams are ordered correctly')


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

    new_topic_consumer: kafka.KafkaConsumer = use_consumer(topic)
    assert new_topic_consumer.bootstrap_connected()
    tests_logger.info(f'Connected to {topic} successfully')

    expected_number_of_messages = len(manually_filtered_messages_names)
    tests_logger.info(f'Expecting {expected_number_of_messages} messages filtered to {topic}')

    new_topic_messages_names: list[str] = []
    stop_iteration_hits = 0
    while len(new_topic_messages_names) < expected_number_of_messages:
        try:
            split_message = next(new_topic_consumer)
            tests_logger.info(f'Got message in {topic}: {split_message.value}')
            if message_name := ast.literal_eval(
                split_message.value.decode('utf-8')
            ).get('name'):
                new_topic_messages_names.append(message_name)
        except StopIteration:
            tests_logger.info(f'Waiting for more messages in {topic}')
            stop_iteration_hits += 1
        if stop_iteration_hits > 3:
            raise TimeoutError(f'Waiting for more messages in {topic} timed out')

    assert new_topic_messages_names == manually_filtered_messages_names
    tests_logger.info('Splitting works as expected')
