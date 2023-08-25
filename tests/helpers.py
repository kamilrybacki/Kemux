import logging

import functools

import lib.splitter.streams.primary
import lib.splitter.streams.secondary


@functools.lru_cache
def get_splitter_output_topics() -> list[str]:
    logging.info(
        stream.Outputs.__dict__.values()
        for stream in [
            lib.splitter.streams.primary,
            lib.splitter.streams.secondary,
        ]
    )

    return [
        topic
        for sublist in [
            [
                output.IO.topic
                for output in stream.Outputs.__dict__.values()
            ]
            for stream in [
                lib.splitter.streams.primary,
                lib.splitter.streams.secondary,
            ]
        ]
        for topic in sublist
    ]
