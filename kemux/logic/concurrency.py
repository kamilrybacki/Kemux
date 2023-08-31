import asyncio
import typing

import kemux.types


def try_in_event_loop(function: typing.Callable, *args, **kwargs) -> None:
    try:
        asyncio.get_event_loop().run_until_complete(
            function(*args, **kwargs)
        )
    except RuntimeError:
        asyncio.run(
            function(*args, **kwargs)
        )


def order_streams(streams: kemux.types.StreamsMap) -> kemux.types.StreamsMap:
    ordered_streams = {}
    for stream_info in find_streams_order([
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


def find_streams_order(info: list[tuple]) -> list[tuple]:
    for stream_index in range(len(info)):
        stream_input_topic = info[stream_index][0]
        for other_stream_index in range(stream_index + 1, len(info)):
            other_stream_output_topics = info[other_stream_index][1]
            if stream_input_topic in other_stream_output_topics:
                info[stream_index], info[other_stream_index] = info[other_stream_index], info[stream_index]
    return info
