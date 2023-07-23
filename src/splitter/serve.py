import faust
import os

import backend.receiver  # type: ignore


def serve() -> None:
    address = os.getenv('SPLITTER_KAFKA_ADDRESS', 'kafka://0.0.0.0:9092')
    topic = os.getenv('SPLITTER_KAFKA_INPUT_TOPIC', 'splitter_input')
    if not address:
        raise ValueError('SPLITTER_KAFKA_ADDRESS is not set')
    if not topic:
        raise ValueError('SPLITTER_KAFKA_INPUT_TOPIC is not set')
    receiver = backend.receiver.Receiver.init(topic, address)
    receiver.start()


if __name__ == '__main__':
    serve()
