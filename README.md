# Kemux - Kafka Demultiplexer

![Documentation Status](https://readthedocs.org/projects/kemux/badge/?version=latest&style=plastic)
![Code linting](https://github.com/kamilrybacki/kemux/actions/workflows/linting.yml/badge.svg)
![Integration tests](https://github.com/kamilrybacki/kemux/actions/workflows/integration.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/python-3.11-green.svg)](https://www.python.org/downloads/release/python-311/)

<img src="docs/assets/images/logo.svg" data-canonical-src="docs/assets/images/logo.svg" width="250"/>

**Kemux** is a Kafka demultiplexer that allows to route messages from one topic to another based on a set of rules.

## How it works

Kemux is a Python library that uses Robinhood's [Faust] to process messages from one topic and route them to another topic based on a set of rules.

The rules are defined within a Stream object, that defines input and output topics, schema of their messages, as well as a set of filters and transformations that are applied to messages.

## Installation

Kemux can be installed via `pip`:

```bash
pip install kemux
```

## Usage

### Running as a standalone application

Kemux can be run as a standalone application, using the `Manager` class:

```python
from kemux.manager import Manager

kemux_manager = Manager(
  ...
)
kemux_manager.start()
```

### Running as a Docker container

Kemux can be deployed as a standalone Docker container. The process is described in the [Docker section] of the documentation.

## Further reading

For more information about Kemux, please refer to the [documentation].

[Faust]: https://faust.readthedocs.io/en/latest/index.html
[Docker section]: https://kemux.readthedocs.io/en/latest/user-guide/docker/
[documentation]: https://kemux.readthedocs.io/en/latest/user-guide/docker/
