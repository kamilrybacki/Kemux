import dataclasses

import kemux.data.processor.input
import kemux.data.processor.output
import kemux.data.schema.input
import kemux.data.schema.output


class Input:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.input.InputSchema):
        _timestamp_ : str
        _name_ : str
        _value_ : int
        _labels_ : dict[str, str]

        @staticmethod
        def _timestamp_validator(timestamp: str) -> None:
            pass

        @staticmethod
        def _name_validator(name: str) -> None:
            if not isinstance(name, str):
                raise ValueError(f'Invalid name: {name}')

        @staticmethod
        def _value_validator(value: int) -> None:
            if not isinstance(value, int):
                raise ValueError(f'Invalid value: {value}')

        @staticmethod
        def _labels_validator(labels: dict[str, str]) -> None:
            if not isinstance(labels, dict):
                raise ValueError(f'Invalid labels: {labels}')

    @dataclasses.dataclass
    class Processor(kemux.data.processor.input.InputProcessor):
        topic = 'animals'

        @staticmethod
        def ingest(message: dict) -> dict:
            return message


class Outputs:
    class Aquatic:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: int

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class Processor(kemux.data.processor.output.OutputProcessor):
            topic = 'aquatic'

            @staticmethod
            def filter(message: dict) -> bool:
                return message.get('name') in ['fish', 'shark']

    class Spooky:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: int

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class Processor(kemux.data.processor.output.OutputProcessor):
            topic = 'spooky'

            @staticmethod
            def filter(message: dict) -> bool:
                return message.get('name') in ['bat', 'spider']

    class Flying:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: int

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class Processor(kemux.data.processor.output.OutputProcessor):
            topic = 'flying'

            @staticmethod
            def filter(message: dict) -> bool:
                return message.get('name') in ['bat', 'bird']
