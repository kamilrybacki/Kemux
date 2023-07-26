import logging
import typing
import re

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class SchemaBase:
    __logger: logging.Logger
    __fields_regex: typing.Pattern[str] = DECORATED_FIELDS_REGEX
    __decorated_fields__: list[str]
    __fields__: list[str]

    @classmethod
    def _find_decorated_fields(cls) -> None:
        if not cls.__decorated_fields__:
            cls.__decorated_fields__ = [*filter(
                cls.__fields_regex.match,
                cls.__dir__()
            )]
        if not cls.__decorated_fields__:
            raise ValueError(f'No fields to validate found in {cls.__name__}')
        cls.__fields__ = [
            field.strip('_')
            for field in cls.__decorated_fields__
        ]
        cls.__logger.info('Found schema fields: %s', ', '.join(cls.__fields__))
        cls._link_fields()

    @classmethod
    def _link_fields(cls) -> None:
        for field in cls.__decorated_fields__:
            cls.__annotations__[field.strip('_')] = cls.__annotations__[field]
            setattr(
                cls,
                field,
                property(
                    fget=lambda self, field=field: getattr(self, field.strip('_')),
                )
            )
