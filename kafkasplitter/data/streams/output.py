import abc
import dataclasses
import logging

import faust
import pydantic

import kafkasplitter.data.schemas.input
import kafkasplitter.data.schemas.output


@dataclasses.dataclass
class OutputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'OUT::{__name__}')
    )
    __topic_handler: faust.TopicT = dataclasses.field(init=False)

    @property
    @abc.abstractmethod
    def topic(self) -> list[str] | str:
        ...

    @property
    @abc.abstractmethod
    def schema(self) -> kafkasplitter.data.schemas.output.OutputSchema:
        ...

    @classmethod
    def process_message(cls, message: kafkasplitter.data.schemas.input) -> None:
        if cls.schema.classify_message(message):
            transformed_message = cls.schema._transform(message)
            cls.logger.info(f'Sending message: {transformed_message} to topic: {cls.topic}')
            cls.__topic_handler.send(value=transformed_message)
