import abc
import dataclasses

import kafkasplitter.data.schema


@dataclasses.dataclass(frozen=True)
class StreamModel(abc.ABC):
    __stream_schema: kafkasplitter.data.schema.OutputSchema = dataclasses.field(init=False)

    @property
    @abc.abstractmethod
    def topic(self) -> str:
        ...

    @classmethod
    def process(cls, message: kafkasplitter.data.schema.InputSchema) -> kafkasplitter.data.schema.OutputSchema:
        return kafkasplitter.data.schema.Output(
            timestamp=message.timestamp,
            value=message.value,
            name=message.name,
            labels=message.labels,
            topic=cls.topic,
        )
