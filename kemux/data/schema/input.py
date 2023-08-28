import dataclasses
import dateutil.parser
import types

import faust
import faust.models.fields

import kemux.data.schema.base


class InputRecordT(kemux.data.schema.base.StreamRecordT):
    def _validate(self) -> None:
        ...


@dataclasses.dataclass
class InputSchema(kemux.data.schema.base.SchemaBase):
    @classmethod
    def _construct_input_record_class(cls) -> None:
        class InputRecord(
            faust.Record,
            serializer='json',
            date_parser=dateutil.parser.parse
        ):
            _decorated_fields: dict[str, type] = faust.models.fields.FieldDescriptor(required=False, exclude=True, default=cls._decorated_fields)  # type: ignore
            _fields: dict[str, type] = faust.models.fields.FieldDescriptor(required=False, exclude=True, default=cls._fields, type=dict)  # type: ignore

            def _validate(self) -> None:
                for field in self._decorated_fields.keys():
                    validator_name = f'{field}validator'
                    validator = getattr(
                        self.__class__,
                        validator_name
                    )
                    if not isinstance(validator, types.FunctionType):
                        raise ValueError(f'Validator: {validator_name} is not callable')
                    actual_field_value = getattr(
                        self,
                        field.strip('_')
                    )
                    validator(actual_field_value)

            def to_dict(self) -> dict:
                return {
                    field_name: getattr(self, field_name)
                    for field_name in self._fields.keys()
                }

        for field_name, field_type in cls._fields.items():
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
        
        cls._logger.info(f'Constructed InputRecord: {InputRecord.__annotations__}')

        implemented_validators = [
            getattr(cls, field)
            for field in cls.__dict__
            if field.endswith('_validator')
        ]
        for validator in implemented_validators:
            if not isinstance(validator, types.FunctionType):
                raise ValueError(f'Validator: {validator} is not callable')
            setattr(InputRecord, validator.__name__, validator)

        cls._record_class = InputRecord  # type: ignore

    @classmethod
    def asdict(cls) -> dict[str, type]:
        return cls._record_class.asdict()
