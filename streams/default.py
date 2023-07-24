import kafkasplitter.data.stream


class Schema(kafkasplitter.data.schema.OutputSchema):
    ...


class Stream(kafkasplitter.data.stream.StreamModel):
    topic = 'default'
