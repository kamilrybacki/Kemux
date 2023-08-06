import logging

import docker
import kafka
import pytest

import conftest


@pytest.mark.order(1)
def test_services_list(compose_file: dict):
    logging.info("Testing containers setup")
    current_containers: set[str] = {
        container.name
        for container in docker.from_env().containers.list()
    }
    logging.info(f"Current containers: {current_containers}")
    expected_containers: set[str] = {
        f"{compose_file['services'][service]['container_name']}"
        for service in compose_file['services']
    }
    logging.info(f"Expected containers: {expected_containers}")
    assert expected_containers == current_containers


@pytest.mark.order(2)
def test_connection_to_kafka_broker(get_consumer: conftest.ConsumerFactory, broker_ip: str):
    consumer: kafka.KafkaConsumer = get_consumer(f'{broker_ip}:9092')
    logging.info("Testing connection to Kafka broker")
    assert consumer.bootstrap_connected()


@pytest.mark.order(3)
def test_topics_exist(get_consumer: conftest.ConsumerFactory, broker_ip: str, topics: set[str]):
    logging.info("Testing topics exist")
    topics_present_in_kafka: set[str] = get_consumer(f'{broker_ip}:9092').topics()
    assert topics.issubset(topics_present_in_kafka)
