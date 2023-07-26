import logging
import re
import typing
from typing import Any

import faust

VALIDATED_FIELDS_REGEX = re.compile(r'^_[a-z]+_$', re.IGNORECASE)


class InputSchema(faust.Record, serializer='json'):
    __logger: logging.Logger
    __fields_regex: typing.Pattern[str] = VALIDATED_FIELDS_REGEX

    def __init__(self, *args: Any, __strict__: bool = True, __faust: Any = None, **kwargs: Any) -> None:
        self.__check_if_there_are_fields_to_validate()
        super().__init__(*args, __strict__=__strict__, __faust=__faust, **kwargs)

    @classmethod
    def __check_if_there_are_fields_to_validate(cls) -> None:
        for field in cls.__annotations__.keys():
            if not any(
                cls.__fields_regex.match(field)
                for field in cls.__annotations__.keys()
            ):
                raise ValueError(f'No fields to validate found in {cls.__name__}')

    def _validate(self) -> None:
        class_fields = set(self.__dir__())  # pylint: disable=unnecessary-dunder-call
        found_schema_fields = self._get_fields_for_validation()
        self.__logger.info('Found schema fields: %s', found_schema_fields)
        for field in found_schema_fields:
            validator_name = f'{field}validator'
            validated = False
            for class_field in class_fields:
                if class_field != validator_name:
                    continue
                validator = getattr(self, validator_name)
                if not isinstance(validator, typing.Callable):
                    raise ValueError(f'Validator: {validator_name} is not callable')
                self.__logger.info('Validating field: %s', field)
                actual_field_value = getattr(
                    self, 
                    field.strip('_')
                )
                validator(actual_field_value)
                class_fields.remove(validator_name)
                class_fields.remove(field)
                validated = True
                break
            if not validated:
                raise ValueError(f'Validator: {validator_name} not found')

    @classmethod
    def _get_fields_for_validation(cls) -> typing.List[str]:
        return [*filter(
            cls.__fields_regex.match,
            cls.__dir__()
        )]

def prepare_input_stream_schema_class(schema: typing.Type[InputSchema]) -> typing.Type[InputSchema]:
    validation_fields = schema._get_fields_for_validation()  # pylint: disable=protected-access
    prepared_input_dict = {
        **schema.__dict__,
        **{
            field.strip('_'): schema.__annotations__[field]
            for field in validation_fields
        },
        'fields': [
            field.strip('_')
            for field in validation_fields
        ]
    }
    return type(
        name=f'InputSchema_{schema.__name__}',
        bases=(InputSchema,),
        dict=prepared_input_dict
    )