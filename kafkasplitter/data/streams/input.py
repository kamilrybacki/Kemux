import abc
import dataclasses
import logging

import faust
import faust.types

import kafkasplitter.data.schemas.input


@dataclasses.dataclass
class InputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'IN::{__name__}')
    )
    _topic_handlers: faust.types.AgentT = dataclasses.field(init=False)

    @property
    @abc.abstractmethod
    def topics(self) -> list[str] | str:
        ...

    @property
    @abc.abstractmethod
    def schema(self) -> kafkasplitter.data.schemas.input.InputSchema:
        ...

    @classmethod
    def validate(cls, message: kafkasplitter.data.schemas.input.InputSchema) -> bool:
        if '_validate' not in message.__dir__():
            raise NotImplementedError(f'Validation runner not found in message! Check the inheritance of {message.__class__} (should be InputSchema)')
        message._validate()  # pylint: disable=protected-access
