import abc
import dataclasses

import faust

import kemux.data.streams.base
import kemux.data.schemas.input
import kemux.data.schemas.output


@dataclasses.dataclass
class OutputStream(kemux.data.streams.base.StreamBase, abc.ABC):
    @classmethod
    def process(cls, message: dict) -> None:
        schema: kemux.data.schemas.output.OutputSchema = cls.schema
        if schema.classify(message):
            transformed_message = schema.transform(message)
            cls.logger.info(f'Sending message: {transformed_message} to topic: {cls.topic}')
            cls._topic_handler.send(value=transformed_message)
