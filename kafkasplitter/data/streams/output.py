import abc
import dataclasses
import logging

import faust

import kafkasplitter.data.schemas.output


@dataclasses.dataclass
class OutputStream(abc.ABC):
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(f'OUT::{__name__}')
    )

    @property
    @abc.abstractmethod
    def topics(self) -> list[str] | str:
        ...

    @property
    @abc.abstractmethod
    def schema(self) -> kafkasplitter.data.schemas.output.OutputSchema:
        ...

    @classmethod
    def process_message(cls, message: faust.Record) -> kafkasplitter.data.schemas.output.OutputSchema:
        return cls.schema(**message)
