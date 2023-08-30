from __future__ import annotations

import dataclasses

import faust

import kemux.data.schema.base
import kemux.data.schema.input


class OutputRecordT(kemux.data.schema.base.SchemaBase):
    pass


@dataclasses.dataclass
class OutputSchema(kemux.data.schema.base.SchemaBase):
    @staticmethod
    def transform(message: dict) -> dict:
        raise NotImplementedError(f'{__name__}.transform() must be implemented!')

    @classmethod
    def construct_output_record_class(cls) -> None:
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
        field_descriptions = cls.record_class.__annotations__.items()
        for field_name, field_type in field_descriptions:
            if field_name not in message:
                return False
            if not isinstance(
                message[field_name],
                field_type
            ):
                return False
        return True
