"""
Functions for loading streams (statically or dynamically).
"""

import os
import types

import importlib.machinery
import inspect

import kemux.data.stream
import kemux.data.processor.base
import kemux.data.processor.input
import kemux.data.processor.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output


def load_streams(streams_dir: str) -> dict[str, kemux.data.stream.Stream]:
    """
    Load all the streams from the given directory.

    Args:
        streams_dir (str): The path to the directory containing the Stream classes.

    Returns:
        dict[str, kemux.data.stream.Stream]: The streams loaded from the given directory.

    Raises:
        ValueError: If the given directory is not a valid directory.
    """

    if not os.path.isdir(streams_dir):  # type: ignore
        raise ValueError(f'Invalid streams directory: {streams_dir}')
    present_modules_filenames = filter(
        lambda module: module.endswith('.py'),
        os.listdir(streams_dir),
    )
    return {
        module_filename.removesuffix('.py'): load_stream_module(streams_dir, module_filename)
        for module_filename in present_modules_filenames
    }


def load_stream_module(
        streams_dir: str,
        module_filename: str
) -> kemux.data.stream.Stream:
    """
    Load a stream module from the given directory.

    Args:
        streams_dir (str): The path to the directory containing the Stream classes.
        module_filename (str): The name of the module file to be loaded.

    Returns:
        kemux.data.stream.Stream: The stream loaded from the given module file.

    Raises:
        ValueError: If the given module file is not a valid stream module.
    """

    module_name = module_filename.removesuffix('.py')
    module_full_path = os.path.join(streams_dir, module_filename)  # type: ignore
    try:
        imported_module: types.ModuleType = importlib.machinery.SourceFileLoader(module_name, module_full_path).load_module()  # pylint: disable=deprecated-method, no-value-for-parameter
    except (OSError, ImportError) as cant_import_stream_module:
        raise ValueError(f'Invalid stream module: {module_name}') from cant_import_stream_module
    if not (stream_input := getattr(imported_module, 'Input', None)):
        raise ValueError(f'No input found for stream module: {module_name}')
    if not (stream_outputs := getattr(imported_module, 'Outputs', None)):
        raise ValueError(f'No outputs found for stream module: {module_name}')
    return kemux.data.stream.Stream(
        input=load_input(stream_input),
        outputs=load_outputs(stream_outputs)
    )


def load_input(input_class: type) -> kemux.data.processor.input.InputProcessor:
    """
    Load an input class from the given class.

    Args:
        input_class (type): The class of the input, containing the Schema and Processor.

    Returns:
        kemux.data.processor.input.InputProcessor: The input processor loaded from the given class.
    """

    input_schema: kemux.data.schema.input.InputSchema
    input_processor: kemux.data.processor.input.InputProcessor

    input_schema, input_processor = get_processor_and_schema(input_class)  # type: ignore
    input_schema.find_decorated_fields()
    input_schema.construct_input_record_class()
    input_processor.schema = input_schema
    return input_processor


def load_outputs(outputs: type) -> dict[str, kemux.data.processor.output.OutputProcessor]:
    """
    Load all the output classes from the given class.

    Args:
        outputs (type): The class of the outputs, each containing a Schema and a Processor.

    Returns:
        dict[str, kemux.data.processor.output.OutputProcessor]: The output processors loaded from the given class.
    """

    return {
        output.topic: output
        for output in [
            load_output(output)
            for output in outputs.__dict__.values()
            if inspect.isclass(output)
        ]
    }


def load_output(output_class: type) -> kemux.data.processor.output.OutputProcessor:
    """
    Load an output class from the given class.

    Args:
        output_class (type): The class of the output, containing the Schema and Processor.

    Returns:
        kemux.data.processor.output.OutputProcessor: The output processor loaded from the given class.
    """

    output_schema: kemux.data.schema.output.OutputSchema
    output_processor: kemux.data.processor.output.OutputProcessor

    output_schema, output_processor = get_processor_and_schema(output_class)  # type: ignore
    output_schema.find_decorated_fields()
    output_schema.construct_output_record_class()
    output_processor.schema = output_schema
    return output_processor


def get_processor_and_schema(source: type) -> tuple[
    kemux.data.schema.base.Schema,
    kemux.data.processor.base.Processor
]:
    """
    Get the schema and processor from the given class.

    Args:
        source (type): The class to get the schema and processor from.

    Returns:
        tuple[kemux.data.schema.base.Schema, kemux.data.processor.base.Processor]: The schema and processor from the given class.

    Raises:
        ValueError: If the given class does not contain a schema or processor.
    """

    schema, io = getattr(  # pylint: disable=invalid-name
        source, 'Schema', None
    ), getattr(
        source, 'Processor', None
    )
    if not schema:
        raise ValueError(f'Invalid input {source.__name__} - no schema found')
    if not io:
        raise ValueError(f'Invalid input {source.__name__} - no io found')
    return schema, io
