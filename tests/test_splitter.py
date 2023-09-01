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
REQUESTED_NUMBER_OF_MESSAGES = 100
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
            raise TimeoutError(f'Filtering function for {topic} timed out (timeout: {FILTERING_TIMEOUT}) seconds')
    tests_logger.info(f'Filtering function for {topic} works as expected')


@pytest.mark.order(7)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_message_splitting(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str):
    producer_consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)
    new_topic_consumer: kafka.KafkaConsumer = use_consumer(topic)
    assert new_topic_consumer.bootstrap_connected()
    tests_logger.info(f'Connected to {topic} successfully')
    next(new_topic_consumer)  # Skip the init message

    outputs_class_name_for_topic = topic.title().replace('-', '')
    filtering_function = helpers.get_filtering_function_for_topic(outputs_class_name_for_topic)

    manually_filtered_messages_names: list[str] = []
    new_topic_messages_names: list[str] = []

    number_of_produced_messages = 0
    while number_of_produced_messages < REQUESTED_NUMBER_OF_MESSAGES:
        produced_json_name = ast.literal_eval(
            next(producer_consumer).value.decode('utf-8')
        ).get('name')
        if filtering_function({
            'name': produced_json_name,
        }):
            manually_filtered_messages_names.append(produced_json_name)
            new_topic_messages_names.append(
                ast.literal_eval(
                    next(new_topic_consumer).value.decode('utf-8')
                ).get('name')
            )
            number_of_produced_messages += 1

    sorted_messages_names = sorted(new_topic_messages_names)
    expected_sorted_messages_names = sorted(manually_filtered_messages_names)

    expected_number_of_messages = len(expected_sorted_messages_names)
    got_number_of_messages = len(sorted_messages_names)

    tests_logger.info(f'Expected {expected_number_of_messages} messages: {expected_sorted_messages_names}')
    tests_logger.info(f'Got {got_number_of_messages} messages: {sorted_messages_names}')

    assert got_number_of_messages == expected_number_of_messages
    assert sorted_messages_names == expected_sorted_messages_names
    tests_logger.info('Splitting works as expected')
