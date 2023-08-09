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
    _logger: logging.Logger = dataclasses.field(init=False)
    _decorated_fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    _fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    _record_class: StreamRecordT = dataclasses.field(init=False)

    @classmethod
    def _find_decorated_fields(cls) -> None:
        cls._decorated_fields = {
            field: cls.__annotations__[field]
            for field in [*filter(
                DECORATED_FIELDS_REGEX.match,
                cls.__annotations__.keys(),
            )]
        }
        if not cls._decorated_fields:
            raise ValueError(f'No fields to validate found in {cls.__name__}')
        cls._fields = {
            field.strip('_'): cls.__annotations__[field]
            for field in cls._decorated_fields
        }
        cls._logger = logging.getLogger(cls.__name__)
        cls._logger.info('Found schema fields: %s', ', '.join(cls._fields))

    @classmethod
    def make_init_message(cls) -> dict:
        return cls._record_class({
            field: None
            for field in cls._fields.keys()
        })
