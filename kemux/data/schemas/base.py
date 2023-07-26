import logging
import typing
import re

import faust

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class SchemaBase(faust.Record, serializer='json'):
    __logger: logging.Logger
    __fields_regex: typing.Pattern[str] = DECORATED_FIELDS_REGEX
    __decorated_fields__: list[str]
    __fields__: list[str]

    def __init__(self, *args: typing.Any, __strict__: bool = True, __faust: typing.Any = None, **kwargs: typing.Any) -> None:
        self._find_decorated_fields()
        super().__init__(*args, __strict__=__strict__, __faust=__faust, **kwargs)

    @classmethod
    def __check_if_there_are_decorated_fields(cls) -> None:
        if not any(
            cls.__fields_regex.match(field)
            for field in cls.__annotations__.keys()
        ):

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
