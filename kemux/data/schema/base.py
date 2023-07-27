import dataclasses
import logging
import re

import faust

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class StreamRecordT(faust.Record):
    _decorated_fields: dict[str, type]
    _fields: dict[str, type]
    def to_dict(self) -> dict:
        ...


@dataclasses.dataclass
class SchemaBase:
    _logger: logging.Logger = dataclasses.field(init=False)
    _decorated_fields: list[str] = dataclasses.field(init=False, default_factory=list)
    _fields: dict[str, type] = dataclasses.field(init=False, default_factory=list)
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
