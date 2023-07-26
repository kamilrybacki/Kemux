import abc
import dataclasses
import importlib.machinery
import logging

import faust

import kemux.data.schemas.input
import kemux.data.streams.input
import kemux.data.schemas.output
import kemux.data.streams.output


@dataclasses.dataclass
class Router():
    app: faust.App
    inputs: dict[str, kemux.data.streams.input.InputStream] = dataclasses.field(init=False, default_factory=dict)
    outputs: dict[str, kemux.data.streams.output.OutputStream]
    __logger: logging.Logger = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        logger_name = f'{self.__class__.__name__}'
        self.__logger = logging.getLogger(logger_name)
        self.__logger.info(f'Loaded stream models: {self.inputs.keys()}')

    def route(
        self,
        input_stream: kemux.data.streams.input.InputStream,
        message: kemux.data.schemas.input.InputSchema
    ) -> None:
        message._validate()
        ingested_message = input_stream.ingest(message)
        for output_stream in self.outputs.values():
            raw_ingested_message = ingested_message.asdict()
            output_stream.process(raw_ingested_message)
