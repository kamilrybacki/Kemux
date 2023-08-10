import logging

import kafka

import conftest

import lib.producer.start

NUMBER_OF_SAMPLES = 20


def test_for_consistency(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory):
    consumer: kafka.KafkaConsumer = use_consumer(lib.producer.start.TEST_TOPIC)
    assert consumer.bootstrap_connected()
    tests_logger.info("Connected to Kafka broker successfully")

    for _ in range(NUMBER_OF_SAMPLES):
        message = next(consumer)
        tests_logger.info(f"Received message: {message}")
