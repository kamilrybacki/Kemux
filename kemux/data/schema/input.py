# pylint: disable=abstract-method
"""
Input schema for Kemux.

This is the schema that is used to validate incoming messages.
"""

import dataclasses
import types

import dateutil.parser
import faust
import faust.models.fields

import kemux.data.schema.base


# pylint: disable=protected-access
@dataclasses.dataclass
class InputSchema(kemux.data.schema.base.Schema):
    """
    InputSchema Class

    The schema that is used to validate incoming messages.
    """

    @classmethod
    def construct_input_record_class(cls) -> None:
        """
        Factory used to construct the faust.Record subclass that is used to accept and validate incoming messages.

        Raises:
            ValueError: If a validator is not a valid callable.
        """

        class InputRecord(
            faust.Record,
            serializer='json',
            date_parser=dateutil.parser.parse
        ):
            """
            InputRecord Class

            The faust.Record that is used to accept and validate incoming messages.
            """

            def validate_message(self) -> None:
                """
                Validate the message using validators defined by the user.
                These validators follow the following naming pattern: "_<field_name>_validator"

                Raises:
                    ValueError: If a validator is not a valid callable.
                """

                message_data = self.__dict__  # pylint: disable=eval-used
                for field in cls.fields:
                    validator_name = f'_{field}_validator'
                    validator = getattr(
                        self.__class__,
                        validator_name
                    )
                    if not isinstance(validator, types.FunctionType):
                        raise ValueError(f'Validator: {validator_name} is not callable')
                    actual_field_value = message_data.get(field)
                    validator(actual_field_value)

            def to_dict(self) -> dict:
                """
                Convert the record to a dict.

                Returns:
                    dict: The input record as a dict.
                """

                return {
                    field: self.__dict__.get(field)
                    for field in cls.fields
                }

        for field_name, field_type in cls.fields.items():
            InputRecord.__annotations__[field_name] = field_type
            setattr(
                InputRecord,
                field_name,
                faust.models.fields.FieldDescriptor(
                    required=True,
                    exclude=False,
                    default=None,
                    type=field_type
                )
            )

        implemented_validators = [
            getattr(cls, field)
            for field in cls.__dict__
            if field.endswith('_validator')
        ]
        for validator in implemented_validators:
            if not isinstance(validator, types.FunctionType):
                raise ValueError(f'Validator: {validator} is not callable')
            setattr(InputRecord, validator.__name__, validator)

        cls.record_class = InputRecord  # type: ignore

    @classmethod
    def asdict(cls) -> dict[str, type]:
        """
        Convert the nested faust.Record subclass to a dict.

        Returns:
            dict: The nested faust.Record subclass as a dict.
        """

        return cls.record_class.asdict()
