from __future__ import annotations

import abc
import typing

import faust

import kemux.data.schemas.base
import kemux.data.schemas.input


class OutputSchema(kemux.data.schemas.base.SchemaBase, abc.ABC):
    __output_constructor__: type[faust.Record]

    @property
    @abc.abstractmethod
    def transformers(self) -> dict[str, typing.Callable[[dict], dict]]:
        ...

    @classmethod
    @abc.abstractmethod
    def classify(cls, message: dict) -> bool:
        ...

    @classmethod
    def transform(cls, message: dict) -> faust.Record:
        transformers: dict[
            type[kemux.data.schemas.input.InputSchema],
            typing.Callable[[dict], dict]
        ] = cls.transformers
        if not (transformer := transformers.get(message.__class__)):
            raise ValueError(f'No transformer found for message: {message}')
        raw_message = message.asdict()
        transformed_message = transformer(message=raw_message)
        return cls.__construct(transformed_message)

    @classmethod
    def __construct(cls, message: dict) -> faust.Record:
        if not cls.__output__constructor__:
            target_fields_annotations = {
                target_field: (target_field_annotation, ...)
                for target_field, target_field_annotation in cls.__annotations__.items()
                if target_field in cls.__fields__
            }
            cls.__output_constructor__ = type(
                cls.__name__,
                (faust.Record),
                **target_fields_annotations,
            )
        return cls.__output_constructor__(**message)
