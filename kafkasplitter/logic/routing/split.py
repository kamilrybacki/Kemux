import dataclasses

import kafkasplitter.logic.routing.route


@dataclasses.dataclass
class Splitter(kafkasplitter.logic.routing.route.Router):
    def process_message(self, message: kafkasplitter.data.schema.InputSchema) -> kafkasplitter.data.schema.Output:
        self.__logger.info(f'Processing message: {message}')
        if not (model := self._streams.get(message.name)):
            self.__logger.warning(f'No model found for message: {message}')
            return kafkasplitter.data.schema.Output(
                timestamp=message.timestamp,
                value=message.value,
                name=message.name,
                labels=message.labels,
                topic='splitter_unsorted',
            )
        return model.process(message)
