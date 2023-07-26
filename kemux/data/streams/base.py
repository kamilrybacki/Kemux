import dataclasses
import logging

import faust
import faust.types

import kemux.data.schemas.base

@dataclasses.dataclass
class StreamBase:
    topic: str = dataclasses.field(init=False)
    schema: kemux.data.schemas.base.BaseSchema = dataclasses.field(init=False)
    logger = dataclasses.field(
        init=False,
        default=logging.getLogger(__name__)
    )
    _topic_handler: faust.types.AgentT | None = dataclasses.field(init=False, default=None)
