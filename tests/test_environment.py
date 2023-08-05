import logging

import docker
import kafka


def test_services_list(docker_compose: dict):
    logging.info("Testing containers setup")
    current_containers: list[str] = {
        container.name
        for container in docker.from_env().containers.list()
    }
    logging.info(f"Current containers: {current_containers}")
    expected_containers: list[str] = {
        f"{docker_compose['services'][service]['container_name']}"
        for service in docker_compose['services']
    }
    logging.info(f"Expected containers: {expected_containers}")
    assert expected_containers == current_containers


def test_connection_to_kafka_broker(consumer: kafka.KafkaConsumer):
    logging.info("Testing connection to Kafka broker")
    assert consumer.bootstrap_connected()


def test_topics_exist(consumer: kafka.KafkaConsumer, topics: list):
    logging.info("Testing topics exist")
    topics_present_in_kafka: list[str] = consumer.topics()
    assert set(topics).issubset(topics_present_in_kafka)
