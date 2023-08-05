import pytest
import kafka

import lib.splitter.streams.primary
import lib.splitter.streams.secondary


@pytest.fixture(scope="session")
def consumer() -> kafka.KafkaConsumer:
    return kafka.KafkaConsumer(
        bootstrap_servers='broker:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tests',
    )


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
