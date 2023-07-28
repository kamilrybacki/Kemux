import os

import kemux.logic.processing


def start() -> None:
    if not (streams_dir := os.getenv('KEMUX_STREAMS_DIR')):
        raise ValueError('KEMUX_STREAMS_DIR environment variable not set')
    if not (data_dir := os.getenv('KEMUX_DATA_DIR')):
        raise ValueError('KEMUX_DATA_DIR environment variable not set')
    if not (kafka_address := os.getenv('KEMUX_KAFKA_ADDRESS')):
        raise ValueError('KEMUX_KAFKA_ADDRESS environment variable not set')
    receiver = kemux.logic.processing.Processor.init(
        kafka_address,
        data_dir,
        streams_dir,
    )
    receiver.start()


if __name__ == '__main__':
    start()
