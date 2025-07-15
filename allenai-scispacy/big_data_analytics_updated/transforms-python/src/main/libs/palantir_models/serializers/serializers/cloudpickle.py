#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import importlib
from types import ModuleType

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class CloudPickleSerializer(ModelSerializer[object]):
    """Serializer utilizing the cloudpickle library for generic objects"""

    file_name = "cloudpickle.pkl"
    cloudpickle: ModuleType

    def __init__(self):
        self.cloudpickle = importlib.import_module("cloudpickle")

    def serialize(self, writer: ModelStateWriter, obj: object):
        with writer.open(self.file_name, "wb") as cloudpickle_file:
            self.cloudpickle.dump(obj, cloudpickle_file)

    def deserialize(self, reader: ModelStateReader) -> object:
        with reader.open(self.file_name, "rb") as cloudpickle_file:
            obj = self.cloudpickle.load(cloudpickle_file)
        return obj
