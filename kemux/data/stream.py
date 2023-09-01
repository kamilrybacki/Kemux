import dataclasses
import logging

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input
import kemux.data.schema.output

import kemux.logic.concurrency


@dataclasses.dataclass
class StreamBase:
    input: kemux.data.io.input.StreamInput | None = dataclasses.field(default=None)
    outputs: dict[str, kemux.data.io.output.StreamOutput] = dataclasses.field(default_factory=dict)
    logger: logging.Logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )

    def process(self, message: kemux.data.schema.input.InputRecordT) -> None:
        raw_message = message.to_dict()
        if '__kemux_init__' in raw_message:
            return
        message.validate()
        ingested_message = self.input.ingest(raw_message)  # type: ignore
        self.logger.info(f'Processing {self.input.topic} message: {ingested_message}')  # type: ignore
        for output in self.outputs.values():
            if output.filter(ingested_message):
                kemux.logic.concurrency.try_in_event_loop(
                    output.send,
                    ingested_message
                )

    def topics(self) -> tuple[str, list[str]]:
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
        stream_input: kemux.data.io.input.StreamInput,
        input_schema: kemux.data.schema.input.InputSchema | None = None,
    ) -> None:
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
        stream_output: kemux.data.io.output.StreamOutput,
        output_schema: kemux.data.schema.output.OutputSchema | None = None,
    ) -> None:
        if output_schema:
            if stream_output.schema:
                self.logger.warning(
                    'Output schema already defined. Overwriting with new schema.'
                )
            stream_output.schema = output_schema
        self.outputs[stream_output.topic] = stream_output

    def remove_output(self, output_topic_name: str) -> None:
        if output_topic_name not in self.outputs:
            self.logger.warning(f'No output found with name: {output_topic_name}')
            return
        del self.outputs[output_topic_name]
        self.logger.info(f'Removed output: {output_topic_name}')
        return


def order_streams(streams: dict[str, kemux.data.stream.StreamBase]) -> dict[str, kemux.data.stream.StreamBase]:
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
    for stream_index, stream_info in enumerate(info):
        stream_input_topic = stream_info[0]
        for other_stream_index, other_stream_info in enumerate(info[stream_index + 1:], start=stream_index + 1):
            other_stream_output_topics = other_stream_info[1]
            if stream_input_topic in other_stream_output_topics:
                info[stream_index], info[other_stream_index] = info[other_stream_index], info[stream_index]
    return info
