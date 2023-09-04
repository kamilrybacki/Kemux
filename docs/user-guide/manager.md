# Managing Kemux streams

Kemux streams can be defined in two ways:

1. Statically, as standalone Python scripts, adhering to the class-based structure [shown previously](streams.md#combining-io-into-a-stream).
2. Programatically, by defining `Processor` and `Schema` subclasses for each of the new streams inputs and outputs.

Both of these methods are a part of the `kemux.manager.Manager` class, which is responsible for loading and running the streams.

## Manager class

To initialize the manager, the `init` class method must be called. It takes the following arguments:

- (*required*) `name`: The name of the manager. This is used to identify the manager in the logs.
- (*required*) `kafka_address`: The address of the Kafka broker.
- (*required*) `persistent_data_directory`: The directory in which `faust.App` instance stores data used by local tables, etc. (see: `datadir` parameter in [Faust documentation](https://faust.readthedocs.io/en/latest/userguide/settings.html#datadir))
- (*optional*) `streams_dir`: The directory where the streams are located. By default it is set to `None`, which means that streams will be loaded programatically after the manager is initialized.

### Loading streams

#### Statically

To load streams statically, a path to the directory containing the streams must be provided in the `streams_dir` argument of the `init` method.

#### Programatically

To load streams programatically, the `add_stream` method can be used. It takes the following arguments:

- (*required*) `name`: The name of the stream. This is used to identify the stream in the logs.
- (*required*) `stream_input_class`: The input of the stream, defined similarly to the way it is done in standalone [stream scripts](streams.md#input) file.
- (*required*) `stream_outputs_class`: The output of the stream, defined similarly to the way it is done in standalone [stream scripts](streams.md#output) file.

**Remember**: Each input/output need to have `Processor` and `Schema` sublasses defined.

### Running streams

After loading the streams, the `start` method must be called to run them. This starts the underlying Faust application and starts routing messages from the input to the output, according to the stream definition.
