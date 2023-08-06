import os
import typing
import yaml
import logging

import docker
import pytest
import kafka

import lib.splitter.streams.primary
import lib.splitter.streams.secondary


COMPOSE_FILE_PATH = os.path.join(os.path.dirname(__file__), "./environment/docker-compose.yml")
TESTS_NETWORK_NAME = "tests"


ConsumerFactory = typing.Callable[[str], kafka.KafkaConsumer]


@pytest.fixture(scope="session")
def get_consumer() -> ConsumerFactory:
    def consumer_factory(broker_address: str) -> kafka.KafkaConsumer:
        return kafka.KafkaConsumer(
            bootstrap_servers=broker_address,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='tests',
        )
    return consumer_factory


@pytest.fixture(scope="session")
def topics() -> list:
    all_outputs = {
        **lib.splitter.streams.primary.Outputs.__dict__,
        **lib.splitter.streams.secondary.Outputs.__dict__,
    }
    return [*map(
        lambda output_class:
            output_class.IO.topic,
        [
            *filter(
                lambda output_class_field:
                    isinstance(output_class_field, type),
                list(all_outputs.values())
            )
        ]
    )]


@pytest.fixture(scope="session")
def compose_file() -> dict:
    with open(COMPOSE_FILE_PATH, "r", encoding="utf-8") as compose_file:
        return yaml.safe_load(compose_file)


@pytest.fixture(scope="session")
def broker_ip(compose_file: dict) -> str:
    broker_container_name: str = f"{compose_file['services']['broker']['container_name']}"
    broker_container = docker.from_env().containers.get(broker_container_name)
    logging.info(f"Broker container: {broker_container.attrs    }")
    broker_ip: str = broker_container.attrs['NetworkSettings']['IPAddress']
    return broker_ip