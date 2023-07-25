import kafkasplitter.data.streams.output
import kafkasplitter.data.schemas.output


class Schema(kafkasplitter.data.schemas.output.OutputSchema):
    ...


class Branch(kafkasplitter.data.streams.output.OutputStream):
    topics = ['aquatic', 'real']
