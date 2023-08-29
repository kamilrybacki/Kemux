from __future__ import annotations

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


@dataclasses.dataclass(kw_only=True)
class Processor:
    kafka_address: str
    streams_dir: str
    persistent_data_directory: str
    _app: faust.App = dataclasses.field(init=False)
    __instance: Processor | None = dataclasses.field(init=False, default=None)
    __logger: logging.Logger = dataclasses.field(init=False, default=logging.getLogger(__name__))
    __streams: dict[str, kemux.data.stream.StreamBase] = dataclasses.field(init=False, default_factory=dict)
    __agents: dict[str, faust.types.AgentT] = dataclasses.field(init=False, default_factory=dict)

    @classmethod
    def init(cls, kafka_address: str, data_dir: str, streams_dir: str) -> Processor:
        if cls.__instance is None:
            instance: Processor = cls(
                kafka_address=kafka_address,
                streams_dir=streams_dir,
                persistent_data_directory=data_dir,
            )
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
            instance.__streams = instance.load_streams()
            instance._app = app
            cls.__instance = instance
        return cls.__instance

    def load_streams(self) -> dict[str, kemux.data.stream.StreamBase]:
        if not os.path.isdir(self.streams_dir):
            raise ValueError(f'Invalid streams directory: {self.streams_dir}')
        present_modules_filenames: filter[str] = filter(
            lambda module: module.endswith('.py'),
            os.listdir(self.streams_dir),
        )
        return {
            module_filename.removesuffix('.py'): self._load_stream_module(module_filename)
            for module_filename in present_modules_filenames
        }

    def _load_stream_module(self, module_filename: str) -> kemux.data.stream.StreamBase:
        module_name = module_filename.removesuffix('.py')
        module_full_path = os.path.join(self.streams_dir, module_filename)
        try:
            imported_module: types.ModuleType = importlib.machinery.SourceFileLoader(module_name, module_full_path).load_module()
        except (OSError, ImportError) as cant_import_stream_module:
            raise ValueError(f'Invalid stream module: {module_name}') from cant_import_stream_module
        if not (input := getattr(imported_module, 'Input', None)):
            raise ValueError(f'No input found for stream module: {module_name}')
        if not (outputs := getattr(imported_module, 'Outputs', None)):
            raise ValueError(f'No outputs found for stream module: {module_name}')
        return kemux.data.stream.StreamBase(
            input=self._load_input(input),
            outputs=self._load_outputs(outputs)
        )

    def _load_input(self, input_class: type) -> kemux.data.io.input.StreamInput:
        input_schema: kemux.data.schema.input.InputSchema
        input_io: kemux.data.io.input.StreamInput

        input_schema, input_io = self._extract_schema_and_io(input_class)  # type: ignore
        input_schema._find_decorated_fields()
        input_schema._construct_input_record_class()
        input_io.schema = input_schema
        return input_io

    def _load_outputs(self, outputs: type) -> list[kemux.data.io.output.StreamOutput]:
            return [
                self._load_output(output)
                for output in outputs.__dict__.values()
                if inspect.isclass(output)
            ]

    def _load_output(self, output_class: type) -> kemux.data.io.output.StreamOutput:
        output_schema: kemux.data.schema.output.OutputSchema
        output_io: kemux.data.io.output.StreamOutput

        output_schema, output_io = self._extract_schema_and_io(output_class)  # type: ignore
        output_schema._find_decorated_fields()
        output_schema._construct_output_record_class()
        output_io.schema = output_schema
        return output_io

    def _extract_schema_and_io(self, source: type) -> tuple[
        kemux.data.schema.base.SchemaBase,
        kemux.data.io.base.IOBase
    ]:
        schema, io = getattr(
            source, 'Schema', None
        ), getattr(
            source, 'IO', None
        )
        if not schema:
            raise ValueError(f'Invalid input {source.__name__} - no schema found')
        if not io:
            raise ValueError(f'Invalid input {source.__name__} - no io found')
        return schema, io

    def start(self) -> None:
        self.__logger.info('Starting receiver')
        stream: kemux.data.stream.StreamBase
        for stream_name, stream in self.__streams.items():
            stream_input: kemux.data.io.input.StreamInput = stream.input
            self.__logger.info(f'Activating input stream: {stream_name}')
            input_topics_handler: faust.TopicT = stream_input._get_handler(self._app)  # pylint: disable=protected-access
            self.__logger.info(f'Activating output streams: {stream_name}')

            # pylint: disable=protected-access, cell-var-from-loop
            async def _process_input_stream_message(messages: faust.StreamT[kemux.data.schema.input.InputSchema]) -> None:
                self.__logger.info('Processing messages')
                output: kemux.data.io.output.StreamOutput
                for output in stream.outputs:
                    output._initialize_handler(self._app)
                    await output._get_handler(self._app).declare()
                async for message in messages:
                    await stream.process(message)  # type: ignore

            self.__logger.info('Activating agent for input stream')
            self.__agents[stream_name] = self._app.agent(input_topics_handler)(_process_input_stream_message)
        self._app.main()
