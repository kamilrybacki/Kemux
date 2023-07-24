import kafkasplitter.data.stream


class StreamSchema(kafkasplitter.data.schema.OutputSchema):
    name: str


class Stream(kafkasplitter.data.stream.StreamModel):
    topic = 'animals'
