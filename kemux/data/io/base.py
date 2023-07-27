import dataclasses
import logging

import faust
import faust.types

import kemux.data.schema.base


@dataclasses.dataclass
class IOBase:
    topic: str = dataclasses.field(init=False)
    schema: kemux.data.schema.base.SchemaBase = dataclasses.field(init=False)
    logger: logging.Logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )
    _topic_handler: faust.types.AgentT | None = dataclasses.field(init=False, default=None)

    @classmethod
    def _get_handler(cls, app: faust.App) -> faust.TopicT:
        if cls._topic_handler is None:
            cls._initialize_handler(app)
        return cls._topic_handler
    
    @classmethod
    def _initialize_handler(cls, app: faust.App) -> None:
        schema: kemux.data.schema.base.SchemaBase = cls.schema
        cls._topic_handler = app.topic(
            cls.topic,
            value_type=schema._record_class,
        )
        cls._topic_handler.maybe_declare()