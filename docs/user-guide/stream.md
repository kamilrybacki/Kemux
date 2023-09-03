# Defining a stream

## Stream structure

A stream is defined by a `Stream` object that contains the `Input` and `Output` objects.

Both `Input` and `Output` objects are defined by a `Schema` and an `IO` object.
These objects  defined in the `kemux` package are `dataclasses`.
Thus, the `dataclasses` package is to be imported when defining a `Stream`.

Knowing that, the `Stream` object is defined as follows:

```python
class Input:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.input.InputSchema):
        ...

    @dataclasses.dataclass
    class IO(kemux.data.io.input.StreamInput):
        ...

class Outputs:
    class OutputTopic1:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            ...

        @dataclasses.dataclass
        class IO(kemux.data.io.output.StreamOutput):
            ...

    class OutputTopic2:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            ...

        @dataclasses.dataclass
        class IO(kemux.data.io.output.StreamOutput):
            ...
    ...
```

Note that the `Input` and `Output` objects are defined as classes, used as namespaces for the `Schema` and `IO` objects.

**It is important to note that the `Schema`/`IO` objects are to be both defined as `dataclasses` and to inherit from correct `kemux` package objects.**

Example: `Input.Schema` inherits from `kemux.data.schema.input.InputSchema` and `Output.IO` inherits from `kemux.data.io.output.StreamOutput`.

### Input

The `Input` object is defined by the `Input.Schema` and `Input.IO` objects.

#### Input.Schema

The `Input.Schema` object is defined by the `kemux.data.schema.input.InputSchema` class.

This class is a `dataclass` that contains the schema of the input data and validation methods for each of input data fields.

An example of `Input.Schema` object is given below:

```python
@dataclasses.dataclass
class Schema(kemux.data.schema.input.InputSchema):
    _field1_: str
    _field2_: int

    def validate_field1(self, field1: str) -> bool:
        return True

    def validate_field2(self, field2: int) -> bool:
        return True
```

Field corresponding to keys present in the input data are prefixed and suffixed by `_` (underscore) characters.

This decoration signifies that the field is to be validated by the `validate_<field_name>` method of the `Input.Schema` object.

In other words, if a field is prefixed and suffixed by `_` characters, the `Input.Schema` object must contain a `validate_<field_name>` method.

Each message read from the input Kafka topic will be validated, ensuring that each of them meets a predefined schema.

If a field is not prefixed and suffixed by `_` characters, it **will not be treated as a field of the input data**,
can be used by the `Schema` object for internal state management across the validation methods.

#### Input.IO

The `Input.IO` object is defined by the `kemux.data.io.input.StreamInput` class
and is used to defined the name of the input Kafka topic and the method used to read (here: *ingest*) messages from the topic.

The name of the input Kafka topic **must be** defined by the `topic` attribute of the `Input.IO` object.

For the ingestation method, the `Input.IO` object must define a `ingest`. This method accepts the message read from the input Kafka topic, represented as a `dict` object, and returns the input data, represented as a `dict` object.

An example of `Input.IO` object is given below:

```python
@dataclasses.dataclass
class IO(kemux.data.io.input.StreamInput):
    topic: str = "input-topic"

    def ingest(self, message: dict) -> dict:
        message["_field1_"] = message["_field1_"].upper()
        return message
```

Combinining the `Input.Schema` and `Input.IO` objects, the `Input` object is defined as follows:

```python
class Input:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.input.InputSchema):
        _field1_: str
        _field2_: int

        def validate_field1(self, field1: str) -> bool:
            return True

        def validate_field2(self, field2: int) -> bool:
            return True

    @dataclasses.dataclass
    class IO(kemux.data.io.input.StreamInput):
        topic: str = "input-topic"

        def ingest(self, message: dict) -> dict:
            message["_field1_"] = message["_field1_"].upper()
            return message
```

### Outputs

The `Outputs` object is defined by a group of output subclasses, where each one possses a structure similar to the `Input` class, by the `Output.Schema` and `Output.IO` objects.

First, a structure of an output subclass will be discussed.

#### Output.Schema

The `Output.Schema` object is defined by the `kemux.data.schema.output.OutputSchema` class. This class is a `dataclass` that contains the schema of the output data and a method used to `transform`` the input data into the output data i.e. one schema to another.

An example of `Output.Schema` object is given below:

```python
@dataclasses.dataclass
class Schema(kemux.data.schema.output.OutputSchema):
    _name_: str
    _value_: int

    @staticmethod
    def transform(message: dict) -> dict:
        return message
```

Field corresponding to keys present in the output data are prefixed and suffixed by `_` (underscore) characters.

#### Output.IO

The `Output.IO` object is defined by the `kemux.data.io.output.StreamOutput` class. This class must define the name of the output Kafka topic and the method used to filter and route the incoming messages, as follows:

```python
@dataclasses.dataclass
class IO(kemux.data.io.output.StreamOutput):
    topic: str = "output-topic"

    def filter(self, message: dict) -> None:
        ...
```

The `filter` method is used to filter the incoming messages, based on the input data schema, and route them to the correct output Kafka topic.

Combining the `Output.Schema` and `Output.IO` objects, the `Output` object is defined as follows:

```python
class OutputTopic1:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.output.OutputSchema):
        _name_: str
        _value_: int

        @staticmethod
        def transform(message: dict) -> dict:
            return message

    @dataclasses.dataclass
    class IO(kemux.data.io.output.StreamOutput):
        topic: str = "output-topic-1"

        def filter(self, message: dict) -> None:
            ...
...
```

As seen [above](#stream-structure), multiple outputs can be defined by defining multiple output subclasses within the `Outputs` class:

```python
class Outputs
  class OutputTopic1:
    ...
  class OutputTopic2:
    ...
```

### Stream

Combining the `Input` and `Output` objects, a new stream can be defined in a python file, as follows:

```python
import dataclasses

import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.input
import kemux.data.schema.output

class Input:
    @dataclasses.dataclass
    class Schema(kemux.data.schema.input.InputSchema):
        _field1_: str
        _field2_: int

        def validate_field1(self, field1: str) -> bool:
            return True

        def validate_field2(self, field2: int) -> bool:
            return True

    @dataclasses.dataclass
    class IO(kemux.data.io.input.StreamInput):
        topic: str = "input-topic"

        def ingest(self, message: dict) -> dict:
            message["_field1_"] = message["_field1_"].upper()
            return message

class Outputs:
    class OutputTopic1:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: int

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class IO(kemux.data.io.output.StreamOutput):
            topic: str = "output-topic-1"

            def filter(self, message: dict) -> None:
                ...

    class OutputTopic2:
        @dataclasses.dataclass
        class Schema(kemux.data.schema.output.OutputSchema):
            _name_: str
            _value_: int

            @staticmethod
            def transform(message: dict) -> dict:
                return message

        @dataclasses.dataclass
        class IO(kemux.data.io.output.StreamOutput):
            topic: str = "output-topic-2"

            def filter(self, message: dict) -> None:
                ...
```
