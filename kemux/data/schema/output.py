"""
Base class for output schema classes.
"""

from __future__ import annotations

import dataclasses

import faust

import kemux.data.schema.base
import kemux.data.schema.input


@dataclasses.dataclass
class OutputSchema(kemux.data.schema.base.Schema):
    """
    OutputSchema Class

    The schema that is used to construct and validate outgoing messages.
    """

    @staticmethod
    def transform(message: dict) -> dict:
        """
        Transform the message into a format that can be consumed by the output schema.

        Must be implemented by user when defining a stream output messages schema.
        This is done instead of enforcing implementation via abstract classes,
        due to the fact that this class is never instantiated directly,
        but rather used as an interface.

        Args:
            message (dict): The message to be transformed.

        Raises:
            NotImplementedError: If the method is not implemented by the user.
        """

        raise NotImplementedError(f'{__name__}.transform() must be implemented!')

    @classmethod
    def construct_output_record_class(cls) -> None:
        """
        Factory used to construct the faust.Record subclass that is used to construct and validate outgoing messages.
        """

        cls.record_class = type(
            cls.__name__,
            (faust.Record, ),
            {
                '__annotations__': {
                    **faust.Record.__annotations__,
                    **cls.fields
                }
            },
        )  # type: ignore

    @classmethod
    def validate(cls, message: dict) -> bool:
        """
        Validate the outgoing message using schema fields defined in the OutputSchema class.

        Args:
            message (dict): The message to be validated.

        Returns:
            bool: True if the message is valid, False otherwise.
        """

        field_descriptions = cls.record_class.__annotations__.items()
        for field_name, field_type in field_descriptions:
            if field_name not in message:
                cls.logger.warning(f'Missing field: {field_name}')
                return False
            if not isinstance(
                message[field_name],
                field_type
            ):
                cls.logger.warning(f'Invalid field type: {field_name} ({field_type})')
                return False
        return True
