from __future__ import annotations

import dataclasses
import logging
import os

import faust
import faust.types

import kafkasplitter.data.schemas.input
import kafkasplitter.data.streams.input
import kafkasplitter.logic.routing.split
import kafkasplitter.logic.routing.ingest

DEFAULT_MODELS_PATH = 'streams'


@dataclasses.dataclass
class Processor:
    kafka_address: str
    persistent_data_directory: str
    streams_dir: str
    _app: faust.App = dataclasses.field(init=False)
    _source_topics: dict[str, faust.TopicT] = dataclasses.field(init=False)
    __instance: Processor | None = dataclasses.field(init=False, default=None)
    __logger: logging.Logger = dataclasses.field(init=False, default=logging.getLogger(__name__))
    __ingester: kafkasplitter.logic.routing.ingest.Ingester = dataclasses.field(init=False)
    __splitter: kafkasplitter.logic.routing.split.Splitter = dataclasses.field(init=False)
    __agents: dict[str, faust.types.AgentT] = dataclasses.field(init=False)

    @classmethod
    def init(cls, kafka_address: str) -> Processor:
        if cls.__instance is None:
            instance = cls(kafka_address)
            instance.__logger.info('Initialized receiver')
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            instance.__logger.info(f'Connecting to Kafka broker: {kafka_address}')
            app = faust.App(
                'splitter_broker',
                broker=kafka_address,
                value_serializer='json',
                datadir=instance.persistent_data_directory,
            )
            input_models, output_models = instance.classify_models()
            instance._app = app
            cls.__instance = instance
        return cls.__instance
    
    @classmethod
    def classify_models(cls) -> tuple[
        list[kafkasplitter.data.streams.input.InputStream],
        list[kafkasplitter.data.streams.output.OutputStream]
    ]:
        if not os.path.isdir(cls.streams_dir):
            raise ValueError(f'Invalid streams directory: {cls.streams_dir}')

    def start(self) -> None:
        self.__logger.info('Starting receiver')
        for stream in self.__ingester.get_streams():
            stream: kafkasplitter.data.streams.input.InputStream
            topics = stream.topics if isinstance(stream.topics, list) else [stream.topics]
            topics_key = '|'.join(topics)
            self.__logger.info(f'Starting handler for topics: {topics_key.replace("|", ", ")}')
            self.__agents[topics_key] = self._agent_factory(topics) 
        self._app.main()
    
    def _agent_factory(self, topics_to_subscribe: list[str]) -> faust.types.AgentT:
        input_topics_handler: faust.TopicT = self.__ingester.get_topics_handler(topics_to_subscribe)
        async def _process_input_stream_message(messages: faust.StreamT[kafkasplitter.data.schema.InputSchema]) -> None:
            self.__logger.info('Processing messages')
            async for message in messages:
                self.__ingester.validate_message(message)
                self.__splitter.process_message(message)
        return self._app.agent(input_topics_handler)(_process_input_stream_message)
