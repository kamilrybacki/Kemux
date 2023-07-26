import abc
import dataclasses

import faust

import kemux.data.streams.base
import kemux.data.schemas.input


@dataclasses.dataclass
class InputStream(kemux.data.streams.base.StreamBase, abc.ABC):
    @classmethod
    @abc.abstractmethod
    def ingest(cls, message: kemux.data.schemas.input.InputSchema) -> kemux.data.schemas.input.InputSchema:
        ...

    @classmethod
    def _get_handler(cls, app: faust.App) -> faust.TopicT:
        if cls._topic_handler is None:
            schema: kemux.data.schemas.input.InputSchema = cls.schema
            cls._topic_handler = app.topic(
                cls.topic,
                value_type=schema.__record_class__,
            )
        return cls._topic_handler
