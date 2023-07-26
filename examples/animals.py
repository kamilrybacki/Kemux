import datetime

import kemux.data.streams.input
import kemux.data.schemas.input


class Schema(kemux.data.schemas.input.InputSchema):
    timestamp: datetime.datetime
    _name_: str
    _value_: float
    _labels_: dict[str, str]

    def _name_validator(self, value: str) -> None:
        if not value:
            raise ValueError('Name cannot be empty')
    
    def _value_validator(self, value: float) -> None:
        if value < 0:
            raise ValueError('Value cannot be negative')
    
    def _labels_validator(self, value: dict[str, str]) -> None:
        pass


class Stream(kemux.data.streams.input.InputStream):
    def ingest(self, message: kemux.data.schemas.input.InputSchema) -> kemux.data.schemas.input.BaseSchema:
        message.labels['stream'] = self.topic
        return message
