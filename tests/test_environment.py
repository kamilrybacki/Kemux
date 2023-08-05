import logging

import docker


def test_containers_setup(topics: list):
    logging.info("Testing containers setup")
    current_containers = docker.from_env().containers.list()
    logging.info(f"Current containers: {current_containers}")
