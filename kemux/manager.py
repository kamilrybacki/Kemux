# pylint: disable=consider-using-enumerate
"""
Manager class for Kemux.

The main class that is used to initialize and start the Kemux receiver.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import typing

import faust
import faust.types

import kemux.data.processor.base
import kemux.data.processor.input
import kemux.data.processor.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output
import kemux.data.stream

import kemux.logic.imports

DEFAULT_MODELS_PATH = 'streams'


@dataclasses.dataclass(kw_only=True)
class Manager:
    """
    Manager Class

    The main class that is used to initialize and start the Kemux receiver
    and operate on the incoming and outgoing messages accordign to the specifications
    defined by the user in the Stream subclasses.

    Attributes:
        name (str): The name of the Kemux application.
        kafka_address (str): The address of the Kafka broker.
        streams_dir (str): The path to the directory containing the Stream subclasses.
        persistent_data_directory (str): The path to the directory where persistent Faust data will be stored.
        logger (logging.Logger): The logger that will be used to log messages.
        agents (dict[str, faust.types.AgentT]): The Faust agents that will be used to process incoming messages.
        streams (dict[str, kemux.data.stream.Stream]): The streams that will be used to process incoming messages.
    """

    name: str
    kafka_address: str
    streams_dir: str | None
    persistent_data_directory: str
    logger: logging.Logger = dataclasses.field(init=False)
    agents: dict[str, faust.types.AgentT] = dataclasses.field(init=False, default_factory=dict)

    _app: faust.App = dataclasses.field(init=False)
    _event_loop: asyncio.AbstractEventLoop = dataclasses.field(init=False, default_factory=asyncio.get_event_loop)

    __instance: Manager | None = dataclasses.field(init=False, default=None)

    @property
    def streams(self) -> dict[str, kemux.data.stream.Stream]:
        return self.__streams

    @streams.setter
    def streams(self, streams: dict[str, kemux.data.stream.Stream]) -> None:
        self.__streams = kemux.data.stream.order_streams(streams)

    @classmethod
    def init(cls, name: str, kafka_address: str, data_dir: str, streams_dir: str | None = None) -> Manager:
        """
        Initialize the Kemux receiver.

        Args:
            name (str): The name of the Kemux application.
            kafka_address (str): The address of the Kafka broker.
            data_dir (str): The path to the directory where persistent Faust data will be stored.
            streams_dir (str, optional): The path to the directory containing the Stream subclasses. Defaults to None.
            If None, the Manager will require the user to add streams manually via the add_stream() method.

        Returns:
            Manager: The initialized Manager instance.
        """

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
                stream_wait_empty=False,
                topic_allow_declare=False,
                topic_disable_leader=True,
                loop=instance._event_loop,
            )
            instance.streams = kemux.logic.imports.load_streams(streams_dir) if streams_dir else {}
            instance._app = app
            instance._app.logger = instance.logger
            cls.__instance = instance
        return cls.__instance

    def add_stream(self, name: str, stream_input_class: type, stream_outputs_class: type) -> None:
        """
        Add a stream to the Kemux manager.

        Args:
            name (str): The name of the stream.
            stream_input_class (type): The class of the stream input, containing the Schema and Processor.
            stream_outputs_class (type): The class of the stream outputs, containing the Schema and Processor.
        """

        stream_input = kemux.logic.imports.load_input(stream_input_class)
        stream_outputs = kemux.logic.imports.load_outputs(stream_outputs_class)
        self.streams = {
            **self.streams,
            name: kemux.data.stream.Stream(
                input=stream_input,
                outputs=stream_outputs,
            )
        }

    def remove_stream(self, name: str) -> None:
        """
        Remove a stream from the Kemux manager.

        Args:
            name (str): The name of the stream.
        """

        if name not in self.streams:
            self.logger.warning(f'No stream found with name: {name}')
            return
        self.streams = {
            stream_name: stream
            for stream_name, stream in self.streams.items()
            if stream_name != name
        }

    def start(self) -> None:
        """
        Start the Kemux receiver i.e. the underlying Faust application and the stream agents.

        Raises:
            ValueError: If no streams have been loaded (either manually or from a directory)
        """

        if not self.streams.keys():
            raise ValueError('No streams have been loaded!')

        self.logger.info('Initializing streams')
        self._event_loop.run_until_complete(
            self.initialize_streams()
        )

        self.logger.info('Starting receiver loop')
        self._app.main()

    async def initialize_streams(self) -> None:
        """
        Initialize the streams i.e. initialize the input and output topic handlers and the stream agents.

        Raises:
            ValueError: If a stream input is not defined or is invalid.
        """

        for stream_name, stream in self.streams.items():
            if (stream_input := stream.input) is None:
                raise ValueError(f'Invalid stream input: {stream_name}')
            self.logger.info(f'{stream_name}: activating input topic handler')

            stream_input.initialize_handler(self._app)
            input_topics_handler: faust.TopicT | None = stream_input.topic_handler
            if not input_topics_handler:
                raise ValueError(f'{stream_name}: invalid {stream_input.topic} input topic handler')

            output: kemux.data.processor.output.OutputProcessor
            for output in stream.outputs.values():
                output.initialize_handler(self._app)
                self.logger.info(f'{stream_name}: activating output topic handler: {output.topic}')
                await output.declare()

            self.logger.info(f'{stream_name}: activating stream agent')
            self.agents[stream_name] = self._app.agent(
                input_topics_handler
            )(
                self.create_processing_function(stream)
            )

    def create_processing_function(self, stream: kemux.data.stream.Stream) -> typing.Callable[[faust.StreamT[kemux.data.schema.input.InputSchema]], typing.Awaitable[None]]:
        """
        Create a processing function for a stream, specific to the name of its input topic.

        Args:
            stream (kemux.data.stream.Stream): The stream to create the processing function for.

        Returns:
            typing.Callable[[faust.StreamT[kemux.data.schema.input.InputSchema]], typing.Awaitable[None]]: The record processing function.
        """

        async def _process_input_stream_message(events: faust.StreamT[kemux.data.schema.input.InputSchema]) -> None:
            event: faust.types.EventT
            async for event in events.events():
                await stream.process(event)
        _process_input_stream_message.__name__ = f'process_{stream.input.topic}_message'  # type: ignore
        _process_input_stream_message.__qualname__ = f'{self.__class__.__name__}.{_process_input_stream_message.__name__}'  # type: ignore
        return _process_input_stream_message
