import dataclasses
import importlib.machinery
import logging
import os

import kafkasplitter.data.schema
import kafkasplitter.data.stream

AvaiableModels = dict[str, kafkasplitter.data.stream.StreamModel]


@dataclasses.dataclass
class Splitter:
    topic: str
    _streams: AvaiableModels = dataclasses.field(init=False, default_factory=dict)
    __logger: logging.Logger = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        logger_name = f'{self.__class__.__name__}::{self.topic}'
        self.__logger = logging.getLogger(logger_name)
        self.load_models()
        self.__logger.info(f'Loaded models: {self._streams.keys()}')

    def load_models(self) -> None:
        if not (models_path := os.environ.get('SPLITTER_STREAMS_PATH')):
            raise ValueError('SPLITTER_STREAMS_PATH environment variable not set')
        self.__logger.info(f'Loading models from {models_path}')
        splitter_models_list = os.listdir(models_path)
        for model_name in splitter_models_list:
            self.__logger.info(f'Loading model: {model_name}')
            if not model_name.endswith('.py'):
                self.__logger.error(f'Invalid model extension: {model_name}')
                continue
            try:
                module_full_path = os.path.join(models_path, model_name)
                module_name = model_name.removesuffix('.py')
                self.__logger.info(f'Importing model from {module_full_path}')
                stream_module = importlib.machinery.SourceFileLoader(module_name, module_full_path).load_module()
            except ImportError as import_error:
                self.__logger.error(f'Error importing model: {model_name} - {import_error}')
                continue
            except OSError as import_error:
                self.__logger.error(f'Error importing model: {model_name} - {import_error}')
                continue
            if not hasattr(stream_module, 'Stream'):
                self.__logger.error(f'No Stream class found in model: {model_name}')
                continue
            if not issubclass(stream_module.Stream, kafkasplitter.data.stream.StreamModel):
                self.__logger.error(f'Invalid Stream class in model: {model_name}')
                continue
            self.__logger.info(f'Adding model: {model_name}')
            self._streams[module_name] = stream_module.Stream  # type: ignore

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
