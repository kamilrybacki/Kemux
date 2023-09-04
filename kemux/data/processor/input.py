"""
Base class for stream input classes.
"""

import dataclasses

import kemux.data.processor.base
import kemux.data.schema.input


@dataclasses.dataclass
class InputProcessor(kemux.data.processor.base.Processor):
    """
    InputProcessor Class

    Provides a common interface for stream input classes to read data from Kafka topic.
    """

    @staticmethod
    def ingest(message: dict) -> dict:
        """
        Ingest a message from a Kafka topic.
        Must be implemented by user when defining a stream input.
        This is done instead of enforcing implementation via abstract classes,
        due to the fact that this class is never instantiated directly,
        but rather used as an interface.

        Args:
            message (dict): Faust record payload deserialized to dict for ingestation, with structure defined by input schema.

        Returns:
            dict: Ingested message.

        Raises:
            NotImplementedError: If not implemented by user.
        """

        raise NotImplementedError(f'{__name__}.ingest() must be implemented!')
