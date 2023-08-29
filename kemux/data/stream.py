import dataclasses

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input


@dataclasses.dataclass
class StreamBase:
    input: kemux.data.io.input.StreamInput
    outputs: list[kemux.data.io.output.StreamOutput]

    async def process(self, message: kemux.data.schema.input.InputRecordT) -> None:
        raw_message = message.to_dict()
        if '__kemux_init__' in raw_message:
            return
        message._validate()  # pylint: disable=protected-access
        ingested_message = self.input.ingest(raw_message)
        for output in self.outputs:
            if output.filter(message):
                await output.send(ingested_message)
