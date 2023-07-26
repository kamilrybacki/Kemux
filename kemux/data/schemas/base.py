import dataclasses
import logging
import re

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


@dataclasses.dataclass
class SchemaBase:
    _logger: logging.Logger = dataclasses.field(init=False)
    _decorated_fields: list[str] = dataclasses.field(init=False, default_factory=list)
    _fields: list[str] = dataclasses.field(init=False, default_factory=list)

    @classmethod
    def _find_decorated_fields(cls) -> None:
        print(cls.__dict__)
        cls._decorated_fields = [*filter(
            DECORATED_FIELDS_REGEX.match,
            cls.__annotations__.keys(),
        )]
        if not cls._decorated_fields:
            raise ValueError(f'No fields to validate found in {cls.__name__}')
        cls._fields = [
            field.strip('_')
            for field in cls._decorated_fields
        ]
        cls._logger = logging.getLogger(cls.__name__)
        cls._logger.info('Found schema fields: %s', ', '.join(cls._fields))
        cls._link_fields()

    @classmethod
    def _link_fields(cls) -> None:
        for field in cls._decorated_fields:
            cls.__annotations__[field.strip('_')] = cls.__annotations__[field]
            setattr(
                cls,
                field,
                property(
                    fget=lambda self, field=field: getattr(self, field.strip('_')),
                )
            )
