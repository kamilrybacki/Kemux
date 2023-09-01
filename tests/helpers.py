import typing

import functools

import lib.splitter.streams.primary
import lib.splitter.streams.secondary


@functools.lru_cache
def get_splitter_output_topics() -> list[str]:
    return [
        topic
        for sublist in [
            [
                output.IO.topic
                for output in stream.Outputs.__dict__.values()
                if hasattr(output, 'IO') and hasattr(output, 'Schema')
            ]
            for stream in [
                lib.splitter.streams.primary,
                lib.splitter.streams.secondary,
            ]
        ]
        for topic in sublist
    ]


@functools.lru_cache
def get_filtering_function_for_topic(topic: str) -> typing.Callable[[dict], bool]:
    try:
        outputs_class = getattr(
            getattr(
                lib.splitter.streams.primary,
                'Outputs',
            ),
            topic
        )
    except AttributeError:
        outputs_class = getattr(
            getattr(
                lib.splitter.streams.secondary,
                'Outputs',
            ),
            topic
        )
    assert isinstance(outputs_class, type)

    return getattr(
        outputs_class,
        'IO',
    ).filter
