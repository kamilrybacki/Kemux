"""
Base class for stream output classes.
"""

import datetime
import dataclasses

import kemux.data.processor.base
import kemux.data.schema.input
import kemux.data.schema.output


@dataclasses.dataclass
class OutputProcessor(kemux.data.processor.base.Processor):
    """
    OutputProcessor Class

    Provides a common interface for stream output classes to write data to Kafka topic.
    """

    @staticmethod
    def filter(message: dict) -> bool:
        """
        Check if a message qualifies for the output topic.
        Must be implemented by user when defining a stream output.
        This is done instead of enforcing implementation via abstract classes,
        due to the fact that this class is never instantiated directly,
        but rather used as an interface.

        Args:
            message (dict): Message to check, with structure defined by schema for input records.

        Returns:
            bool: True if message qualifies for output topic, False otherwise.

        Raises:
            NotImplementedError: If not implemented by user.
        """

        raise NotImplementedError(f'{__name__}.filter() must be implemented!')

    @classmethod
    async def send(cls, message: dict) -> None:
        """
        Send a message to the output topic.

        Args:
            message (dict): Message to send, with structure defined by schema for output records.
        """

        transformed_message = cls.schema.transform(message=message)  # type: ignore
        cls.logger.info(f'Sending message: {transformed_message} to {cls.topic}')
        if cls.schema.validate(message=transformed_message):  # type: ignore
            await cls.topic_handler.send(value=transformed_message)  # type: ignore
        else:
            cls.logger.warning(f'Invalid message: {message}')

    @classmethod
    async def declare(cls) -> None:
        """
        Declare the output topic and send an init message.

        The init message is used to ensure that the topic is created before any messages are sent.
        It is always sent as the first message to the topic with the following structure:

        ```python
        {
            '__kemux_init__': <current datetime in ISO format>
        }
        ```

        Raises:
            ValueError: If invalid topic handler.
        """

        if not cls.topic_handler:
            raise ValueError(f'Invalid {cls.topic} output topic handler')
        await cls.topic_handler.declare()
        cls.logger.info(f'Sending init message to {cls.topic}')
        await cls.topic_handler.send(
            value={
                '__kemux_init__': datetime.datetime.now().isoformat()
            }
        )
