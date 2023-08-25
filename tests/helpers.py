
def verify_schema(schema: dict[str, type], data: dict) -> None:
    for key, value in schema.items():
        assert key in data
        assert data[key] is value

