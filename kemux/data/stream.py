import dataclasses
import logging

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input
import kemux.data.schema.output


@dataclasses.dataclass
class StreamBase:
    input: kemux.data.io.input.StreamInput | None = dataclasses.field(default=None)
    outputs: list[kemux.data.io.output.StreamOutput] = dataclasses.field(default_factory=list)
    logger: logging.Logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )

    async def process(self, message: kemux.data.schema.input.InputRecordT) -> None:
        raw_message = message.to_dict()
        if '__kemux_init__' in raw_message:
            return
        message.validate()
        ingested_message = self.input.ingest(raw_message)  # type: ignore
        for output in self.outputs:
            if output.filter(message):
                await output.send(ingested_message)

    def topics(self) -> tuple[str, list[str]]:
        if self.input:
            return (
                self.input.topic, [
                    output.topic
                    for output in self.outputs
                ]
            )
        raise ValueError('Stream not initialized. Check your inputs and outputs configuration.')

    def add_input(
        self,
        stream_input: kemux.data.io.input.StreamInput,
        input_schema: kemux.data.schema.input.InputSchema | None = None,
    ) -> None:
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
        self.outputs.append(stream_output)

    def remove_output(self, output_topic_name: str) -> None:
        for output in self.outputs:
            if output.topic == output_topic_name:
                self.outputs.remove(output)
                self.logger.info(f'Removed output: {output_topic_name}')
                return
