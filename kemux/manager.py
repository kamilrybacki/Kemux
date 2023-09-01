# pylint: disable=consider-using-enumerate
from __future__ import annotations

import dataclasses
import logging

from concurrent.futures import ThreadPoolExecutor

import faust
import faust.types

import kemux.data.io.base
import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output
import kemux.data.stream

import kemux.logic.imports


DEFAULT_MODELS_PATH = 'streams'


@dataclasses.dataclass(kw_only=True)
class Manager:
    name: str
    kafka_address: str
    streams_dir: str | None
    persistent_data_directory: str
    logger: logging.Logger = dataclasses.field(init=False)
    agents: dict[str, faust.types.AgentT] = dataclasses.field(init=False, default_factory=dict)

    _app: faust.App = dataclasses.field(init=False)

    __instance: Manager | None = dataclasses.field(init=False, default=None)

    @property
    def streams(self) -> dict[str, kemux.data.stream.StreamBase]:
        return self.__streams

    @streams.setter
    def streams(self, streams: dict[str, kemux.data.stream.StreamBase]) -> None:
        self.__streams = kemux.data.stream.order_streams(streams)

    @classmethod
    def init(cls, name: str, kafka_address: str, data_dir: str, streams_dir: str | None = None) -> Manager:
        if cls.__instance is None:
            instance: Manager = cls(
                name=name,
                kafka_address=kafka_address,
                streams_dir=streams_dir,
                persistent_data_directory=data_dir,
            )
            instance.logger = faust.app.base.logger
            instance.logger.info('Initialized receiver')
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            instance.logger.info(f'Connecting to Kafka broker: {kafka_address}')
            app = faust.App(
                name,
                broker=kafka_address,
                value_serializer='json',
                datadir=instance.persistent_data_directory,
                stream_wait_empty=False
            )
            instance.streams = kemux.logic.imports.load_streams(streams_dir) if streams_dir else {}
            instance._app = app
            instance._app.logger = instance.logger
            cls.__instance = instance
        return cls.__instance

    def add_stream(self, name: str, stream_input_class: type, stream_outputs_class: type) -> None:
        stream_input = kemux.logic.imports.load_input(stream_input_class)
        stream_outputs = kemux.logic.imports.load_outputs(stream_outputs_class)
        self.streams = {
            **self.streams,
            name: kemux.data.stream.StreamBase(
                input=stream_input,
                outputs=stream_outputs,
            )
        }

    def remove_stream(self, name: str) -> None:
        if name not in self.streams:
            self.logger.warning(f'No stream found with name: {name}')
            return
        self.streams = {
            stream_name: stream
            for stream_name, stream in self.streams.items()
            if stream_name != name
        }

    def start(self) -> None:
        if not self.streams.keys():
            raise ValueError('No streams have been loaded!')

        self.logger.info('Starting receiver')
        stream: kemux.data.stream.StreamBase
        for stream_name, stream in self.streams.items():
            if (stream_input := stream.input) is None:
                raise ValueError(f'Invalid stream input: {stream_name}')
            self.logger.info(f'{stream_name}: activating input topic handler')
            stream_input.initialize_handler(self._app)

            output: kemux.data.io.output.StreamOutput
            for output_name, output in stream.outputs.items():
                output.initialize_handler(self._app)
                if not output.topic_handler:
                    raise ValueError(f'{stream_name}: invalid {output_name} output topic handler')
                kemux.logic.concurrency.try_in_event_loop(
                    output.topic_handler.declare
                )

            # pylint: disable=cell-var-from-loop
            async def _process_input_stream_message(messages: faust.StreamT[kemux.data.schema.input.InputSchema]) -> None:
                self.logger.info(f'{stream_name}: activating output topic handlers')
                async for message in messages:
                    await stream.process(message)  # type: ignore

            input_topics_handler: faust.TopicT | None = stream_input.topic_handler
            if not input_topics_handler:
                raise ValueError(f'{stream_name}: invalid {stream_input.topic} input topic handler')
            _process_input_stream_message.__name__ = stream_input.topic

            self.logger.info(f'{stream_name}: activating stream agent')
            self.agents[stream_name] = self._app.agent(input_topics_handler)(_process_input_stream_message)

        self.logger.info('Starting receiver loop')
        for agent in self.agents.values():
            kemux.logic.concurrency.try_in_event_loop(
                agent.start
            )

        self._app.main()
