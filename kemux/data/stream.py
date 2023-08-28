import dataclasses

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input


@dataclasses.dataclass
class StreamBase:
    input: kemux.data.io.input.StreamInput
    outputs: list[kemux.data.io.output.StreamOutput]

    async def process(self, message: kemux.data.schema.input.InputRecordT) -> None:
        self.input.logger.info(message.__dict__)
        message._validate()
        raw_message = message.to_dict()
        ingested_message = self.input.ingest(raw_message)
        for output in self.outputs:
            if output.filter(message):
                await output.send(ingested_message)
