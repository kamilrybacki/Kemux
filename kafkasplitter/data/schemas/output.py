import abc
import pydantic

import kafkasplitter.data.schemas.input


class OutputSchemaTransformer(abc.ABC):
    input: kafkasplitter.data.schemas.input.InputSchema
    output: pydantic.BaseModel

    @abc.abstractmethod
    @classmethod
    def translate(cls) -> pydantic.BaseModel:
        ...


class OutputSchema(pydantic.BaseModel, abc.ABC):
    @property
    @abc.abstractmethod
    def key(self) -> str:
        ...
    
    @property
    @abc.abstractmethod
    def transformers(self) -> dict[
        type[kafkasplitter.data.schemas.input.InputSchema],
        type[OutputSchemaTransformer]
    ]:
        ...
    
    @classmethod
    @abc.abstractmethod
    def classify_message(cls, message: kafkasplitter.data.schemas.input.InputSchema) -> bool:
        ...

    @classmethod
    def _transform(cls, message: kafkasplitter.data.schemas.input.InputSchema) -> pydantic.BaseModel:
        if not (transformer := cls.transformers.get(message.__class__)):
            raise ValueError(f'No transformer found for message: {message}')
        return transformer.translate(message)
