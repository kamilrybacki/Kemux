import asyncio
import typing


def try_in_event_loop(function: typing.Callable, *args, **kwargs) -> None:
    try:
        asyncio.get_event_loop().run_until_complete(
            function(*args, **kwargs)
        )
    except RuntimeError:
        asyncio.run(
            function(*args, **kwargs)
        )
