import datetime
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
        transformed_message = cls.schema.transform(message=message)  # type: ignore
        cls.logger.info(f'Sending message: {transformed_message} to {cls.topic}')
        if cls.schema.validate(message=transformed_message):  # type: ignore
            await cls.topic_handler.send(value=transformed_message)  # type: ignore
        else:
            cls.logger.warning(f'Invalid message: {message}')

    @classmethod
    async def declare(cls) -> None:
        if not cls.topic_handler:
            raise ValueError(f'Invalid {cls.topic} output topic handler')
        await cls.topic_handler.declare()
        cls.logger.info(f'Sending init message to {cls.topic}')
        await cls.topic_handler.send(
            value={
                '__kemux_init__': datetime.datetime.now().isoformat()
            }
        )
