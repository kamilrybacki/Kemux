"""
Stream class definition.

A stream is a collection of input and output processors that are connected
and ensure that messages are ingested, transformed, and sent to the correct
output topics, while keeping the data schema consistent to the user's
specifications.
"""

import dataclasses
import logging

import faust.types

import kemux.data.processor.input
import kemux.data.processor.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output


@dataclasses.dataclass
class Stream:
    """
    Stream Class

    A class that connects the input processor with any number of output processors.

    Attributes:
        input (kemux.data.processor.input.InputProcessor): The input processor that will be used to ingest messages.
        outputs (dict[str, kemux.data.processor.output.OutputProcessor]): The output processors that will be used to send messages.
        logger (logging.Logger): The logger that will be used to log messages.
    """

    input: kemux.data.processor.input.InputProcessor | None = dataclasses.field(default=None)
    outputs: dict[str, kemux.data.processor.output.OutputProcessor] = dataclasses.field(default_factory=dict)
    logger: logging.Logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )

    async def process(self, event: faust.types.EventT) -> None:
        """
        Process incoming messages as separate Events coming from input Kafka topic.

        Args:
            event (faust.types.EventT): The event to be processed.
        """

        message: kemux.data.schema.base.StreamRecordT = event.value  # type: ignore
        raw_message = message.to_dict()
        if '__kemux_init__' in raw_message:
            return
        self.logger.info(f'Processing {event.message.topic} message: {raw_message}')  # type: ignore
        message.validate_message()
        ingested_message = self.input.ingest(raw_message)  # type: ignore
        for output in self.outputs.values():
            if output.filter(ingested_message):
                await output.send(ingested_message)

    def topics(self) -> tuple[str, list[str]]:
        """
        Get the input and output topics of the stream.

        Returns:
            tuple[str, list[str]]: The input and output topics of the stream.

        Raises:
            ValueError: If the stream is not initialized and the input stream is not defined.
        """

        if self.input:
            return (
                self.input.topic, [
                    output.topic
                    for output in self.outputs.values()
                ]
            )
        raise ValueError('Stream not initialized. Check your inputs and outputs configuration.')

    def set_input(
        self,
        stream_input: kemux.data.processor.input.InputProcessor,
        input_schema: kemux.data.schema.input.InputSchema | None = None,
    ) -> None:
        """
        Set the input processor of the stream.

        Args:
            stream_input (kemux.data.processor.input.InputProcessor): The input processor to be used.
            input_schema (kemux.data.schema.input.InputSchema, optional): The input schema to be used. Defaults to None.
            This is only used if the input processor does not have a schema defined or if it is to be overwritten.
        """

        if self.input:
            self.logger.warning(
                'Input already defined. Overwriting with new input.'
            )
        self.input = stream_input
        if input_schema:
            if self.input.schema:
                self.logger.warning(
                    'Input schema already defined. Overwriting with new schema.'
                )
            self.input.schema = input_schema

    def add_output(
        self,
        stream_output: kemux.data.processor.output.OutputProcessor,
        output_schema: kemux.data.schema.output.OutputSchema | None = None,
    ) -> None:
        """
        Add an output processor to the stream.

        Args:
            stream_output (kemux.data.processor.output.OutputProcessor): The output processor to be added.
            output_schema (kemux.data.schema.output.OutputSchema, optional): The output schema to be used. Defaults to None.
            The behaviour of this argument is the same as the input_schema argument in the set_input method.
        """

        if output_schema:
            if stream_output.schema:
                self.logger.warning(
                    'Output schema already defined. Overwriting with new schema.'
                )
            stream_output.schema = output_schema
        self.outputs[stream_output.topic] = stream_output

    def remove_output(self, output_topic_name: str) -> None:
        """
        Remove an output processor from the stream.

        Args:
            output_topic_name (str): The name of the output processor to be removed.
        """

        if output_topic_name not in self.outputs:
            self.logger.warning(f'No output found with name: {output_topic_name}')
            return
        del self.outputs[output_topic_name]
        self.logger.info(f'Removed output: {output_topic_name}')
        return


def order_streams(streams: dict[str, Stream]) -> dict[str, Stream]:
    """
    Order the streams based on their dependency on each other.

    Args:
        streams (dict[str, Stream]): The streams to be ordered.

    Returns:
        dict[str, Stream]: The ordered streams.
    
    Raises:
        ValueError: If a stream input is not defined.
    """

    ordered_streams = {}
    for stream_info in find_streams_order([
        stream.topics()
        for stream in streams.values()
    ]):
        input_topic = stream_info[0]
        for stream_name, stream in streams.items():
            if stream.input is None:
                raise ValueError(f'Invalid stream input: {stream_name}')
            if stream.input.topic == input_topic:
                ordered_streams[stream_name] = stream
                break
    return ordered_streams


def find_streams_order(info: list[tuple]) -> list[tuple]:
    """
    Find the order of the streams based on their dependency on each other.

    Args:
        info (list[tuple]): The input and output topics of the streams.

    Returns:
        list[tuple]: The ordered input and output topics of the streams.
    """

    for stream_index, stream_info in enumerate(info):
        stream_input_topic = stream_info[0]
        for other_stream_index, other_stream_info in enumerate(info[stream_index + 1:], start=stream_index + 1):
            other_stream_output_topics = other_stream_info[1]
            if stream_input_topic in other_stream_output_topics:
                info[stream_index], info[other_stream_index] = info[other_stream_index], info[stream_index]
    return info
