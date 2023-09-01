import dataclasses
import logging
import re

import faust

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class StreamRecordT(faust.Record):
    _decorated_fields: dict[str, type]
    _fields: dict[str, type]
    def to_dict(self) -> dict:  # type: ignore
        ...


@dataclasses.dataclass
class SchemaBase:
    logger: logging.Logger = dataclasses.field(init=False)
    decorated_fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    record_class: StreamRecordT = dataclasses.field(init=False)

    @classmethod
    def find_decorated_fields(cls) -> None:
        cls.decorated_fields = {
            field: cls.__annotations__[field]
            for field in [*filter(
                DECORATED_FIELDS_REGEX.match,
                cls.__annotations__.keys(),
            )]
        }
        if not cls.decorated_fields:
            raise ValueError(f'No fields to validate found in {cls.__name__}')
        cls.fields = {
            field.strip('_'): cls.__annotations__[field]
            for field in cls.decorated_fields
        }
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.info('Found schema fields: %s', ', '.join(cls.fields))

    @classmethod
    def make_init_message(cls, topic: str) -> dict:
        cls.logger.info(f'Sending initial message to topic: {topic}')
        initial_message_content = {
            field_name: field_type()
            for field_name, field_type in cls.fields.items()
        }
        return cls.record_class.from_data({
            '__kemux_init__': True,
            **initial_message_content
        })
