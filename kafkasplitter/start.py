import os

import kafkasplitter.logic.receiver


def start() -> None:
    address = os.getenv('SPLITTER_KAFKA_ADDRESS', 'kafka://0.0.0.0:9092')
    topic = os.getenv('SPLITTER_KAFKA_INPUT_TOPIC', 'splitter_input')
    if not address:
        raise ValueError('SPLITTER_KAFKA_ADDRESS is not set')
    if not topic:
        raise ValueError('SPLITTER_KAFKA_INPUT_TOPIC is not set')
    receiver = kafkasplitter.logic.receiver.Receiver.init(topic, address)
    receiver.start()


if __name__ == '__main__':
    start()
