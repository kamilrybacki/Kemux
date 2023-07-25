import abc
import dataclasses
import importlib.machinery
import logging
import os

import kafkasplitter.data.schema
import kafkasplitter.data.streams.input
import kafkasplitter.data.streams.output

AnyStream = kafkasplitter.data.streams.input.InputStream | kafkasplitter.data.streams.output.OutputStream


@dataclasses.dataclass
class Router(abc.ABC):
    _streams: dict[str, AnyStream] = dataclasses.field(init=False, default_factory=dict)
    __logger: logging.Logger = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        logger_name = f'{self.__class__.__name__}'
        self.__logger = logging.getLogger(logger_name)
        self.load_streams()
        self.__logger.info(f'Loaded models: {self._streams.keys()}')

    def load_streams(self, models_list: list[str]) -> None:
        for model_name in models_list:
            self.__logger.info(f'Loading model: {model_name}')
            if not model_name.endswith('.py'):
                self.__logger.error(f'Invalid model extension: {model_name}')
                continue
            try:
                module_full_path = os.path.join(self.__streams_dir, model_name)
                module_name = model_name.removesuffix('.py')
                self.__logger.info(f'Importing model from {module_full_path}')
                stream_module = importlib.machinery.SourceFileLoader(module_name, module_full_path).load_module()
            except (ImportError, OSError) as import_error:
                self.__logger.error(f'Error importing model: {model_name} - {import_error}')
                continue
            self.__logger.info(f'Adding model: {model_name}')
            self._streams[module_name] = stream_module.Stream  # type: ignore
