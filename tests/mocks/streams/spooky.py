import dataclasses

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input
import kemux.data.schema.output


class Input:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.input.InputSchema):
        _name_: str
        _value_: float

        @staticmethod
        def _name_validator(name: str) -> None:
            if not isinstance(name, str):
                raise ValueError(f'Invalid name: {name}')

        @staticmethod
        def _value_validator(value: int) -> None:
            if not isinstance(value, int):
                raise ValueError(f'Invalid value: {value}')

    @dataclasses.dataclass
    class IO(kemux.data.io.input.StreamInput):
        topic = 'spooky'

        @staticmethod
        def ingest(message: dict) -> dict:
            return message


class Outputs:
    class VerySpooky:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: float

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class IO(kemux.data.io.output.StreamOutput):
            topic = 'very-spooky'

            @staticmethod
            def filter(message: dict) -> bool:
                return message.get('name') in ['spider']
