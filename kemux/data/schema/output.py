from __future__ import annotations

import abc
import dataclasses
import typing

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
    def _construct_output_record_class(cls) -> None:
        if not getattr(cls, '_output_constructor', None):
            target_fields_annotations = {
                target_field: (target_field_annotation, ...)
                for target_field, target_field_annotation in cls.__annotations__.items()
                if target_field in cls._fields
            }
            cls._record_class = type(
                cls.__name__,
                (faust.Record, ),
                {'__annotations__': target_fields_annotations},
            )
