#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import importlib
from types import ModuleType

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class DillSerializer(ModelSerializer[object]):
    """Serializer utilizing the dill library for generic objects"""

    file_name = "dill.pkl"
    dill: ModuleType

    def __init__(self):
        self.dill = importlib.import_module("dill")

    def serialize(self, writer: ModelStateWriter, obj: object):
        try:
            with writer.open(self.file_name, "wb") as dill_file:
                self.dill.dump(obj, dill_file, recurse=True)
        except Exception as exc:
            raise ValueError(
                f"Serialization failed for object {obj}. Please ensure that the inputs to your model adapter are valid serializable objects and can be serialized with dill,"
                "or refer to the model adapter serialization documentation for library-specific serialization methods: https://www.palantir.com/docs/foundry/integrate-models/serialization \n"
                f"Original exception is: {exc}"
            ) from exc

    def deserialize(self, reader: ModelStateReader) -> object:
        try:
            with reader.open(self.file_name, "rb") as dill_file:
                obj = self.dill.load(dill_file)
            return obj
        except Exception as exc:
            raise ValueError(
                f"Deserialization failed for object {obj}. Please create a new model version ensuring that the inputs to your model adapter are valid serializable objects and can be serialized with dill,"
                "or refer to the model adapter serialization documentation for library-specific serialization methods: https://www.palantir.com/docs/foundry/integrate-models/serialization \n"
                f"Original exception is: {exc}"
            ) from exc
