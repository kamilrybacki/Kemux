import os

import kemux.manager


def start_splitter() -> None:
    if not (streams_dir := os.getenv('STREAMS_DIR')):
        raise ValueError('STREAMS_DIR environment variable not set')
    if not (data_dir := os.getenv('DATA_DIR')):
        raise ValueError('DATA_DIR environment variable not set')
    if not (kafka_address := os.getenv('KAFKA_ADDRESS')):
        raise ValueError('KAFKA_ADDRESS environment variable not set')
    receiver = kemux.manager.Manager.init(
        'test_splitter',
        kafka_address,
        data_dir,
        streams_dir,
    )
    receiver.start()


if __name__ == '__main__':
    start_splitter()
