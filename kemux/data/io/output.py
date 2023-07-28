import dataclasses

import kemux.data.io.base
import kemux.data.schema.input
import kemux.data.schema.output


@dataclasses.dataclass
class StreamOutput(kemux.data.io.base.IOBase):
    @staticmethod
    def filter(message: dict) -> bool:
        raise NotImplementedError(f'{__name__}.filter() must be implemented!')

    @classmethod
    async def send(cls, message: dict) -> None:
        if cls.filter(message):
            await cls._topic_handler.send(value=message)
