import logging

import functools

import lib.splitter.streams.primary
import lib.splitter.streams.secondary

import kemux.data.schema.output
import kemux.data.io.output


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
