# pylint: disable=consider-using-enumerate
from __future__ import annotations

import asyncio
import dataclasses
import importlib.machinery
import inspect
import logging
import os
import types

import faust
import faust.types

import kemux.data.io.base
import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output
import kemux.data.stream

DEFAULT_MODELS_PATH = 'streams'

StreamsMap = dict[str, kemux.data.stream.StreamBase]


@dataclasses.dataclass(kw_only=True)
class Processor:
    kafka_address: str
    streams_dir: str | None
    persistent_data_directory: str
    logger: logging.Logger = dataclasses.field(init=False, default=logging.getLogger(__name__))
    agents: dict[str, faust.types.AgentT] = dataclasses.field(init=False, default_factory=dict)

    _app: faust.App = dataclasses.field(init=False)

    __instance: Processor | None = dataclasses.field(init=False, default=None)

    @property
    def streams(self) -> StreamsMap:
        return self.__streams

    @streams.setter
    def streams(self, streams: StreamsMap) -> None:
        self.__streams = self.order_streams(streams)

    @classmethod
    def init(cls, kafka_address: str, data_dir: str, streams_dir: str | None = None) -> Processor:
        if cls.__instance is None:
            instance: Processor = cls(
                kafka_address=kafka_address,
                streams_dir=streams_dir,
                persistent_data_directory=data_dir,
            )
            instance.logger.info('Initialized receiver')
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            instance.logger.info(f'Connecting to Kafka broker: {kafka_address}')
            app = faust.App(
                'splitter_broker',
                broker=kafka_address,
                value_serializer='json',
                datadir=instance.persistent_data_directory,
            )
            instance.streams = instance.load_streams() if streams_dir else {}
            instance._app = app
            cls.__instance = instance
        return cls.__instance

    def load_streams(self) -> StreamsMap:
        if not os.path.isdir(self.streams_dir):  # type: ignore
            raise ValueError(f'Invalid streams directory: {self.streams_dir}')
        present_modules_filenames: filter[str] = filter(
            lambda module: module.endswith('.py'),
            os.listdir(self.streams_dir),
        )
        return {
            module_filename.removesuffix('.py'): self.load_stream_module(module_filename)
            for module_filename in present_modules_filenames
        }

    def load_stream_module(self, module_filename: str) -> kemux.data.stream.StreamBase:
        module_name = module_filename.removesuffix('.py')
        module_full_path = os.path.join(self.streams_dir, module_filename)  # type: ignore
        try:
            imported_module: types.ModuleType = importlib.machinery.SourceFileLoader(module_name, module_full_path).load_module()  # pylint: disable=deprecated-method, no-value-for-parameter
        except (OSError, ImportError) as cant_import_stream_module:
            raise ValueError(f'Invalid stream module: {module_name}') from cant_import_stream_module
        if not (stream_input := getattr(imported_module, 'Input', None)):
            raise ValueError(f'No input found for stream module: {module_name}')
        if not (stream_outputs := getattr(imported_module, 'Outputs', None)):
            raise ValueError(f'No outputs found for stream module: {module_name}')
        return kemux.data.stream.StreamBase(
            input=self.load_input(stream_input),
            outputs=self.load_outputs(stream_outputs)
        )

    def load_input(self, input_class: type) -> kemux.data.io.input.StreamInput:
        input_schema: kemux.data.schema.input.InputSchema
        input_io: kemux.data.io.input.StreamInput

        input_schema, input_io = self.get_schema_and_io(input_class)  # type: ignore
        input_schema.find_decorated_fields()
        input_schema.construct_input_record_class()
        input_io.schema = input_schema
        return input_io

    def load_outputs(self, outputs: type) -> dict[str, kemux.data.io.output.StreamOutput]:
        return {
            output.topic: output
            for output in [
                self.load_output(output)
                for output in outputs.__dict__.values()
                if inspect.isclass(output)
            ]
        }

    def load_output(self, output_class: type) -> kemux.data.io.output.StreamOutput:
        output_schema: kemux.data.schema.output.OutputSchema
        output_io: kemux.data.io.output.StreamOutput

        output_schema, output_io = self.get_schema_and_io(output_class)  # type: ignore
        output_schema.find_decorated_fields()
        output_schema.construct_output_record_class()
        output_io.schema = output_schema
        return output_io

    def get_schema_and_io(self, source: type) -> tuple[
        kemux.data.schema.base.SchemaBase,
        kemux.data.io.base.IOBase
    ]:
        schema, io = getattr(  # pylint: disable=invalid-name
            source, 'Schema', None
        ), getattr(
            source, 'IO', None
        )
        if not schema:
            raise ValueError(f'Invalid input {source.__name__} - no schema found')
        if not io:
            raise ValueError(f'Invalid input {source.__name__} - no io found')
        return schema, io

    def add_stream(self, name: str, stream_input_class: type, stream_outputs_class: type) -> None:
        stream_input = self.load_input(stream_input_class)
        stream_outputs = self.load_outputs(stream_outputs_class)
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
        
        self.logger.info(self.streams)

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
                try:
                    asyncio.get_event_loop().run_until_complete(
                        output.topic_handler.declare()
                    )
                except RuntimeError:
                    asyncio.run(
                        output.topic_handler.declare()
                    )

            # pylint: disable=cell-var-from-loop
            async def _process_input_stream_message(messages: faust.StreamT[kemux.data.schema.input.InputSchema]) -> None:
                self.logger.info(f'{stream_name}: activating output topic handlers')
                async for message in messages:
                    await stream.process(message)  # type: ignore

            input_topics_handler: faust.TopicT | None = stream_input.topic_handler
            if not input_topics_handler:
                raise ValueError(f'{stream_name}: invalid {stream_input.topic} input topic handler')

            self.logger.info(f'{stream_name}: activating stream agent')
            _process_input_stream_message.__name__ = stream_input.topic
            self.agents[stream_name] = self._app.agent(input_topics_handler)(_process_input_stream_message)
        self.logger.info('Starting receiver loop')
        self._app.main()

    def order_streams(self, streams: StreamsMap) -> StreamsMap:
        ordered_streams = {}
        for stream_info in self.find_streams_order([
            stream.topics()
            for stream in streams.values()
        ]):
            input_topic = stream_info[0]
            for stream_name, stream in streams.items():
                if stream.input is None:
                    raise ValueError(f'Invalid stream input: {stream_name}')
                if stream.input.topic == input_topic:
                    ordered_streams[stream_name] = stream
                    break
        return ordered_streams

    @staticmethod
    def find_streams_order(info: list[tuple]) -> list[tuple]:
        for stream_index in range(len(info)):
            stream_input_topic = info[stream_index][0]
            for other_stream_index in range(stream_index + 1, len(info)):
                other_stream_output_topics = info[other_stream_index][1]
                if stream_input_topic in other_stream_output_topics:
                    info[stream_index], info[other_stream_index] = info[other_stream_index], info[stream_index]
        return info
