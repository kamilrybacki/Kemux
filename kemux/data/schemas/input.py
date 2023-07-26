import typing

import kemux.data.schemas.base


class InputSchema(kemux.data.schemas.base.SchemaBase):
    def _validate(self) -> None:
        class_fields = set(self.__dir__())  # pylint: disable=unnecessary-dunder-call
        for field in self.__decorated_fields__:
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
