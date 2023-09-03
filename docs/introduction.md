# Introduction

## Installation and setup

### Requirements

This package requires Python 3.11 or higher.

### Python interface installation

Kemux is a Python package that can be installed via `pip`:

```bash
pip install kemux
```

## General concepts

The main entrypoint for using Kemux is the `Manager` class that imports the predefined routing strategies and manage the [`Faust`](https://faust.readthedocs.io/en/latest/index.html) application,
used to consume and send messages from and to various Kafka Topics.

These input/output strategies are called `Streams` in Kemux.
Each stream is composed of the following elements:

1. `Input`: an umbrella class that defines the input messages ingestion and validation
    - `Schema`: a class that defines the input messages field types and methods used to validate the subsequent input messages
    - `IO`: a class that defines the input messages ingestion, i.e. the Kafka topic to consume from and their preprocessing
2. `Output`: an umbrella class that defines the output messages serialization and sending
    - `Schema`: a class that defines the output messages field types and methods used to serialize the subsequent output messages
    - `IO`: a class that defines how to filter the incoming messages i.e. when to send a qualified message to a specific Kafka output topic

These objects can be contained in external modules and imported by the `Manager` by pointing it to the directory containing them or constructed programmatically and passed to the `Manager` via appropriate methods.

At first, it is **important** to understand the [structure of a Kemux stream](user-guide/stream.md).
