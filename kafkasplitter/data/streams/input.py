import abc
import dataclasses
import logging

import faust
import faust.types

import kafkasplitter.data.schemas.input


@dataclasses.dataclass
class InputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'IN::{__name__}')
    )
    _topics_handler: faust.types.AgentT | None = dataclasses.field(init=False, default=None)

    @property
    @abc.abstractmethod
    def topics(self) -> list[str] | str:
        ...

    @property
    @abc.abstractmethod
    def schema(self) -> kafkasplitter.data.schemas.input.InputSchema:
        ...

    @property
    @classmethod
    def id(cls) -> str:
        return '|'.join(cls.topics) if isinstance(cls.topics, list) else cls.topics
    
    @property
    @classmethod
    def targets(cls) -> str:
        return cls.id().replace('|', ', ')

    @classmethod
    def _get_handler(cls, app: faust.App) -> faust.TopicT:
        if cls._topics_handler is None:
            schema_with_validators = kafkasplitter.data.schemas.input.prepare_input_stream_schema_class(cls.schema)
            cls._topics_handler = app.topic(
                cls.topics,
                value_type=schema_with_validators
            )
        return cls._topics_handler
