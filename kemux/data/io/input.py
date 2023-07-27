import dataclasses

import faust

import kemux.data.io.base
import kemux.data.schema.input


@dataclasses.dataclass
class StreamInput(kemux.data.io.base.IOBase):
    @staticmethod
    def ingest(message: dict) -> dict:
        raise NotImplementedError(f'{__name__}.recieve() must be implemented!')

