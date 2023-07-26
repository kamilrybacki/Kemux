import abc
import dataclasses
import logging

import faust
import pydantic

import kemux.data.schemas.input
import kemux.data.schemas.output


@dataclasses.dataclass
class OutputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'OUT::{__name__}')
    )
    topic: str = dataclasses.field(init=False)
    _topic_handler: faust.TopicT = dataclasses.field(init=False)

    @property
    @abc.abstractmethod
    def schema(self) -> kemux.data.schemas.output.OutputSchema:
        ...

    @classmethod
    def process(cls, message: kemux.data.schemas.input.BaseSchema) -> None:
        schema: kemux.data.schemas.output.OutputSchema = cls.schema
        if schema.classify(message):
            transformed_message = schema.transform(message)
            cls.logger.info(f'Sending message: {transformed_message} to topic: {cls.topic}')
            cls._topic_handler.send(value=transformed_message)
