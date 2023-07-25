import os

import kafkasplitter.logic.processing


def start() -> None:
    data_dir = os.getenv('SPLITTER_DATA_DIR')
    streams_dir = os.getenv('SPLITTER_STREAMS_DIR')
    if not data_dir:
        raise ValueError('SPLITTER_DATA_DIR environment variable not set')
    receiver = kafkasplitter.logic.processing.Processor.init(
        os.getenv('SPLITTER_KAFKA_ADDRESS', 'kafka://0.0.0.0:9092'),
    )
    receiver.start()


if __name__ == '__main__':
    start()
