import os

import kemux.logic.processing

def start() -> None:
    if not (streams_dir := os.getenv('SPLITTER_STREAMS_DIR')):
        raise ValueError('SPLITTER_STREAMS_DIR environment variable not set')
    if not (data_dir := os.getenv('SPLITTER_DATA_DIR')):
        raise ValueError('SPLITTER_DATA_DIR environment variable not set')
    if not (kafka_address := os.getenv('SPLITTER_KAFKA_ADDRESS')):
        raise ValueError('SPLITTER_KAFKA_ADDRESS environment variable not set')
    receiver = kemux.logic.processing.Processor.init(
        kafka_address,
        data_dir,
        streams_dir,
    )
    receiver.start()


if __name__ == '__main__':
    start()
