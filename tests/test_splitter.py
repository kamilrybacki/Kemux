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
    try:
        outputs_class = getattr(
            getattr(
                lib.splitter.streams.primary,
                'Outputs',
            ),
            outputs_class_name_for_topic
        )
    except AttributeError:
        outputs_class = getattr(
            getattr(
                lib.splitter.streams.secondary,
                'Outputs',
            ),
            outputs_class_name_for_topic
        )
    assert isinstance(outputs_class, type)

    filtering_function: typing.Callable[[dict], bool] = getattr(
        outputs_class,
        'IO',
    ).filter
    assert filtering_function.__annotations__.get('message')
    tests_logger.info(f'Filtering function for {topic} found')

    filtering_start_time = time.time()
    produced_message = next(producer_consumer)
    produced_json = ast.literal_eval(
        produced_message.value.decode('utf-8')
    )
    while not filtering_function(produced_json):
        produced_json = ast.literal_eval(
            produced_message.value.decode('utf-8')
        )
        tests_logger.info(f'{topic}: {produced_json["name"]}')
        if time.time() - filtering_start_time > FILTERING_TIMEOUT:
            raise TimeoutError(f'Filtering function for {topic} timed out (timeout: {FILTERING_TIMEOUT})')
    tests_logger.info(f'Filtering function for {topic} works as expected')


@pytest.mark.order(7)
@pytest.mark.parametrize('topic', helpers.get_splitter_output_topics())
def test_for_message_splitting(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory, topic: str):
    producer_consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)

    produced_messages_names: list[str] = []
    while set(produced_messages_names) != {*lib.producer.start.POSSIBLE_KEYS}:
        produced_message = next(producer_consumer)
        produced_json = ast.literal_eval(
            produced_message.value.decode('utf-8')
        )
        if '__faust' not in produced_json:
            produced_messages_names.append(
                produced_json.get('name')
            )
        break
