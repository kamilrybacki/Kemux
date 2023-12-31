import logging
import os
import typing
import yaml

import docker
import pytest
import kafka

import streams.primary
import streams.secondary


COMPOSE_FILE_PATH = os.path.join(os.path.dirname(__file__), "./environment/docker-compose.yml")
TESTS_NETWORK_NAME = "environment_default"
TESTS_LOGGER_NAME = "Kemux (integration tests)"


ConsumerFactory = typing.Callable[[str], kafka.KafkaConsumer]


@pytest.fixture(scope='session')
def tests_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    return logging.getLogger(TESTS_LOGGER_NAME)


@pytest.fixture(scope="session")
def compose_file() -> dict:
    with open(COMPOSE_FILE_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


@pytest.fixture(scope="session")
def broker_ip(compose_file: dict) -> str:
    broker_container_name: str = f"{compose_file['services']['broker']['container_name']}"
    broker_container = docker.from_env().containers.get(broker_container_name)
    return broker_container.attrs['NetworkSettings']['Networks'][TESTS_NETWORK_NAME]['IPAddress']


@pytest.fixture(scope="session")
def use_consumer(broker_ip: str) -> ConsumerFactory:
    def consumer_factory(topic: str | None = None) -> kafka.KafkaConsumer:
        if topic is None:
            return kafka.KafkaConsumer(
                bootstrap_servers=broker_ip,
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000,
            )
        return kafka.KafkaConsumer(
            topic,
            bootstrap_servers=broker_ip,
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
        )
    return consumer_factory


@pytest.fixture(scope="session")
def topics() -> set[str]:
    all_outputs = {
        **lib.splitter.streams.primary.Outputs.__dict__,
        **lib.splitter.streams.secondary.Outputs.__dict__,
    }
    return {*map(
        lambda output_class:
            output_class.Processor.topic,
        [
            *filter(
                lambda output_class_field:
                    isinstance(output_class_field, type),
                list(all_outputs.values())
            )
        ]
    )}
