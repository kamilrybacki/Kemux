import abc
import dataclasses
import logging

import faust
import faust.types

import kemux.data.schemas.input


@dataclasses.dataclass
class InputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'IN::{__name__}')
    )
    topic: str = dataclasses.field(init=False)
    _topic_handler: faust.types.AgentT | None = dataclasses.field(init=False, default=None)

    @property
    @abc.abstractmethod
    def schema(self) -> kemux.data.schemas.input.BaseSchema:
        ...

    @classmethod
    def ingest(cls, message: kemux.data.schemas.input.BaseSchema) -> kemux.data.schemas.input.BaseSchema:
        return message

    @classmethod
    def _get_handler(cls, app: faust.App) -> faust.TopicT:
        if cls._topic_handler is None:
            schema_with_validators = kemux.data.schemas.input.prepare_stream_schema_class(cls.schema)
            cls._topic_handler = app.topic(
                cls.topic,
                value_type=schema_with_validators
            )
        return cls._topic_handler
