"""
Base class for Schema classes.
"""

import dataclasses
import logging
import re

import faust

DECORATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class StreamRecordT(faust.Record):
    """
    StreamRecordT Class

    A type hint for custom faust.Record classes.
    These subclasses are created automatically by the input and output schema classes
    and are passed to faust.TopicT initializers as the value_type argument i.e. record models.
    """

    _decorated_fields: dict[str, type]
    _fields: dict[str, type]

    def to_dict(self) -> dict:  # type: ignore
        """
        Convert the record to a dict.

        Returns:
            dict: The record as a dict.
        """


@dataclasses.dataclass
class Schema:
    """
    Schema base Class

    Provides a common interface for Schema classes to validate data and create faust.Record classes.

    Attributes:
        logger (logging.Logger): The logger to use for logging messages.
        decorated_fields (dict[str, type]): The fields to validate, with the leading and trailing underscores.
        fields (dict[str, type]): The fields to validate, without the leading and trailing underscores.
        record_class (faust.Record): The faust.Record class to use for automatic serialization and deserialization of records.
    """

    logger: logging.Logger = dataclasses.field(init=False)
    decorated_fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    fields: dict[str, type] = dataclasses.field(init=False, default_factory=dict)
    record_class: StreamRecordT = dataclasses.field(init=False)

    @classmethod
    def find_decorated_fields(cls) -> None:
        """
        Find decorated fields and compose a faust.Record subclass.

        Raises:
            ValueError: If no decorated fields are found.
        """

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
