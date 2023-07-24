import dataclasses
import datetime

import faust


class InputSchema(faust.Record, serializer='json'):
    timestamp: datetime.datetime
    value: float
    name: str
    labels: dict[str, str]


class OutputSchema:
    timestamp: datetime.datetime
    value_serializer: str = 'json'
    
    def _validate(self) -> None:
        ...
