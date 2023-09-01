import logging

import docker
import kafka
import pytest

import conftest


@pytest.mark.order(1)
def test_services_list(tests_logger: logging.Logger, compose_file: dict):
    tests_logger.info("Testing containers setup")
    current_containers: set[str] = {
        container.name
        for container in docker.from_env().containers.list()
    }
    tests_logger.info(f"Current containers: {current_containers}")
    expected_containers: set[str] = {
        f"{compose_file['services'][service]['container_name']}"
        for service in compose_file['services']
    }
    tests_logger.info(f"Expected containers: {expected_containers}")
    assert expected_containers == current_containers


@pytest.mark.order(2)
def test_connection_to_kafka_broker(tests_logger: logging.Logger, use_consumer: conftest.ConsumerFactory):
    consumer: kafka.KafkaConsumer = use_consumer()
    tests_logger.info("Testing connection to Kafka broker")
    assert consumer.bootstrap_connected()
