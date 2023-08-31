import os
import types

import importlib.machinery
import inspect

import kemux.data.stream
import kemux.data.io.base
import kemux.data.io.input
import kemux.data.io.output
import kemux.data.schema.base
import kemux.data.schema.input
import kemux.data.schema.output


def load_streams(streams_dir: str) -> dict[str, kemux.data.stream.StreamBase]:
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
) -> kemux.data.stream.StreamBase:
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
    return kemux.data.stream.StreamBase(
        input=load_input(stream_input),
        outputs=load_outputs(stream_outputs)
    )


def load_input(input_class: type) -> kemux.data.io.input.StreamInput:
    input_schema: kemux.data.schema.input.InputSchema
    input_io: kemux.data.io.input.StreamInput

    input_schema, input_io = get_schema_and_io(input_class)  # type: ignore
    input_schema.find_decorated_fields()
    input_schema.construct_input_record_class()
    input_io.schema = input_schema
    return input_io


def load_outputs(outputs: type) -> dict[str, kemux.data.io.output.StreamOutput]:
    return {
        output.topic: output
        for output in [
            load_output(output)
            for output in outputs.__dict__.values()
            if inspect.isclass(output)
        ]
    }


def load_output(output_class: type) -> kemux.data.io.output.StreamOutput:
    output_schema: kemux.data.schema.output.OutputSchema
    output_io: kemux.data.io.output.StreamOutput

    output_schema, output_io = get_schema_and_io(output_class)  # type: ignore
    output_schema.find_decorated_fields()
    output_schema.construct_output_record_class()
    output_io.schema = output_schema
    return output_io


def get_schema_and_io(source: type) -> tuple[
    kemux.data.schema.base.SchemaBase,
    kemux.data.io.base.IOBase
]:
    schema, io = getattr(  # pylint: disable=invalid-name
        source, 'Schema', None
    ), getattr(
        source, 'IO', None
    )
    if not schema:
        raise ValueError(f'Invalid input {source.__name__} - no schema found')
    if not io:
        raise ValueError(f'Invalid input {source.__name__} - no io found')
    return schema, io
