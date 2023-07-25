import dataclasses

import faust

import kafkasplitter.data.streams.input
import kafkasplitter.data.schemas.input
import kafkasplitter.logic.routing.route


@dataclasses.dataclass
class Ingester(kafkasplitter.logic.routing.route.Router):
    def get_streams(self) -> list[kafkasplitter.data.streams.input.InputStream]:
        ...

    def validate_message(self, message: kafkasplitter.data.schemas.input.InputSchema) -> None:
        ...

    def get_topics_handler(self, topics: list[str]) -> faust.TopicT:
        ...