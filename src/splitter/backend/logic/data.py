import datetime

import faust


class Input(faust.Record, serializer='json'):
    timestamp: datetime.datetime
    value: float
    name: str
    labels: dict[str, str]
