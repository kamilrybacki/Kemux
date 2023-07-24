from __future__ import annotations

import dataclasses
import logging

import faust
import faust.types

import kafkasplitter.data.schema
import kafkasplitter.logic.split


@dataclasses.dataclass
class Receiver:
    source_topic_name: str
    kafka_address: str
    _app: faust.App = dataclasses.field(init=False)
    _source_topic: faust.TopicT = dataclasses.field(init=False)
    __instance: Receiver | None = dataclasses.field(init=False, default=None)
    __logger: logging.Logger = dataclasses.field(init=False, default=logging.getLogger(__name__))
    __splitter: kafkasplitter.logic.split.Splitter = dataclasses.field(init=False)
    __topic_handler: faust.types.AgentT = dataclasses.field(init=False)

    @classmethod
    def init(cls, topic: str, kafka_address: str) -> Receiver:
        if cls.__instance is None:
            instance = cls(topic, kafka_address)
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            instance.__logger.info('Initialized receiver')
            instance.__splitter = kafkasplitter.logic.split.Splitter(
                instance.source_topic_name,
            )
            instance.__logger.info(f'Connecting to Kafka broker: {kafka_address}')
            app = faust.App(
                'splitter_broker',
                broker=kafka_address,
                value_serializer='json',
            )
            instance._app = app
            instance._source_topic = app.topic(
                topic,
                key_type=str,
                value_type=kafkasplitter.data.schema.InputSchema,
            )
            instance.__logger.info(f'Subscribed to main input topic: {topic}')
            cls.__instance = instance
        return cls.__instance

    def start(self) -> None:
        self.__logger.info('Starting receiver')
        self.__topic_handler = self._app.agent(self._source_topic)(self._process_input_stream_message)
        self._app.main()

    async def _process_input_stream_message(self, messages: faust.StreamT[kafkasplitter.data.schema.InputSchema]) -> None:
        self.__logger.info('Processing messages')
        async for message in messages:
            processed_message = self.__splitter.process_message(message)
            self.__logger.info(f'Processed message: {processed_message}')
            self.__topic_handler.send(
                value=processed_message,
            )
