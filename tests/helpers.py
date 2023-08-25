
def verify_schema(schema: dict, data: dict) -> None:
    for key, value in schema.items():
        assert key in data
        assert isinstance(data[key], value)
