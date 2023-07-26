import dataclasses

import kemux.data.streams.output
import kemux.data.schemas.output


@dataclasses.dataclass
class Schema(kemux.data.schemas.output.OutputSchema):
    _name_: str
    _value_: int
    _labels_: dict[str, str]


class Stream(kemux.data.streams.output.OutputStream):
    topic = 'aquatic'

    @classmethod
    def classify(cls, message: Schema) -> bool:
        return message._name_ in ('fish', 'shark', 'whale')
