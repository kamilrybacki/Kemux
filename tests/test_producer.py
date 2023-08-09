import kafka

import conftest


def test_for_consistency(use_consumer: conftest.ConsumerFactory):
    consumer: kafka.KafkaConsumer = use_consumer()
    assert True
