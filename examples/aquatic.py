import kemux.data.streams.output
import kemux.data.schemas.output

import examples.animals as animals


class Schema(kemux.data.schemas.output.OutputSchema):
    ...


class AnimalToAquatic(kemux.data.schemas.output.OutputSchemaTransformer):
    @classmethod
    def transform(cls, message: dict) -> dict:
        return message


class Branch(kemux.data.streams.output.OutputStream):
    topic = 'aquatic'

    @classmethod
    def classify(cls, message: animals.AnimalsSchema) -> bool:
        return message._name_ in ('fish', 'shark', 'whale')
