import kafkasplitter.data.streams.input
import kafkasplitter.data.schemas.input


class Schema(kafkasplitter.data.schemas.input.InputSchema):
    ...


class Stream(kafkasplitter.data.streams.input.InputStream):
    topics = ['animals']
