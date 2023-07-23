from __future__ import annotations

import dataclasses
import logging

import faust

import logic.data  # type: ignore
import logic.split  # type: ignore


@dataclasses.dataclass
class Receiver:
    source_topic_name: str
    kafka_address: str
    _app: faust.App = dataclasses.field(init=False)
    _source_topic: faust.TopicT = dataclasses.field(init=False)
    __instance: Receiver | None = dataclasses.field(init=False, default=None)
    __logger: logging.Logger = dataclasses.field(init=False, default=logging.getLogger(__name__))
    __splitter: logic.split.Splitter = dataclasses.field(init=False)

    @classmethod
    def init(cls, topic: str, kafka_address: str) -> Receiver:
        if cls.__instance is None:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            app = faust.App(
                'splitter_broker',
                broker=kafka_address,
                value_serializer='json',
            )
            cls.__instance = cls(topic, kafka_address)
            cls.__instance._app = app
            cls.__logger.info(f'Connecting to Kafka broker: {kafka_address}')
            cls.__instance._source_topic = app.topic(
                topic,
                key_type=str,
                value_type=logic.data.Input,
            )
            cls.__instance.__logger.info(f'Subscribed to main input topic: {topic}')
            cls.__instance.__splitter = logic.split.Splitter()
            cls.__logger.info('Initialized receiver')
        return cls.__instance

    def start(self) -> None:
        self.__logger.info('Starting receiver')
        self._app.agent(self._source_topic)(self._process_input_stream_message)
        self._app.main()

    async def _process_input_stream_message(self, messages: faust.StreamT[logic.data.Input]) -> None:
        self.__logger.info('Processing messages')
        async for message in messages:
            message_name = message.name
            self.__logger.info(f'Processing message: {message_name}')
