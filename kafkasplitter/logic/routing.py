import abc
import dataclasses
import importlib.machinery
import logging

import faust

import kafkasplitter.data.schemas.input
import kafkasplitter.data.streams.input
import kafkasplitter.data.schemas.output
import kafkasplitter.data.streams.output


@dataclasses.dataclass
class Router():
    app: faust.App
    inputs: dict[str, kafkasplitter.data.streams.input.InputStream] = dataclasses.field(init=False, default_factory=dict)
    outputs: dict[str, kafkasplitter.data.streams.output.OutputStream]
    __logger: logging.Logger = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        logger_name = f'{self.__class__.__name__}'
        self.__logger = logging.getLogger(logger_name)
        self.__logger.info(f'Loaded stream models: {self.inputs.keys()}')

    def process_message(self, message: kafkasplitter.data.schemas.input.InputSchema) -> None:
        message._validate()
        for output_stream in self.outputs.values():
            output_stream: kafkasplitter.data.streams.output.OutputStream
            output_stream.process_message(message)
