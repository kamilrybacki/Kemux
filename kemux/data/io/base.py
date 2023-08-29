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
    _topic_handler: faust.types.TopicT = dataclasses.field(init=False)

    @classmethod
    def _initialize_handler(cls, app: faust.App) -> None:
        schema: kemux.data.schema.base.SchemaBase = cls.schema
        cls.logger.info(f'Handler schema for {cls.topic}: {schema._record_class.__annotations__}')
        cls._topic_handler = app.topic(
            cls.topic,
            value_type=schema._record_class,
            allow_empty=True,
        )
        cls.logger.info(f'{cls.topic}: topic handler initialized')
