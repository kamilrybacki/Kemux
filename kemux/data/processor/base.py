"""
Base class for Processor classes.
"""

import dataclasses
import logging

import faust
import faust.types

import kemux.data.schema.base


@dataclasses.dataclass
class Processor:
    """
    Processor base Class

    Provides a common interface for Processor classes to read and write data to or from Kafka topics.

    Attributes:
        topic (str): The Kafka topic to read and write data to or from.
        schema (kemux.schema.input.InputSchema): The schema to use for reading and writing data, used by Faust to serialize and deserialize data.
        logger (logging.Logger): The logger to use for logging messages.
        topic_handler (faust.topic.TopicT): The Faust topic handler for the Kafka topic.
    """

    topic: str = dataclasses.field(init=False)
    schema: kemux.data.schema.base.Schema = dataclasses.field(init=False)
    logger: logging.Logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )
    topic_handler: faust.types.TopicT = dataclasses.field(init=False)

    @classmethod
    def initialize_handler(cls, app: faust.App) -> None:
        """
        Initialize the Faust topic handler for the Kafka topic.

        Args:
            app (faust.App): The Faust application to initialize the topic handler for.

        Returns:
            None
        """

        schema: kemux.data.schema.base.Schema = cls.schema
        cls.logger.info(f'Handler schema for {cls.topic}: {schema.record_class.__annotations__}')
        cls.topic_handler = app.topic(
            cls.topic,
            value_type=schema.record_class,
            allow_empty=True,
        )
        cls.logger.info(f'{cls.topic}: topic handler initialized')
