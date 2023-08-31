import dataclasses
import logging

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input
import kemux.data.schema.output


@dataclasses.dataclass
class StreamBase:
    input: kemux.data.io.input.StreamInput | None = dataclasses.field(default=None)
    outputs: dict[str, kemux.data.io.output.StreamOutput] = dataclasses.field(default_factory=dict)
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
        self.logger.info(f'Processing message: {ingested_message}')
        for output in self.outputs.values():
            if output.filter(ingested_message):
                await output.send(ingested_message)

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
