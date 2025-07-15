#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import importlib
from types import ModuleType
from typing import Dict

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class JsonSerializer(ModelSerializer[Dict]):
    """Serializer for json-convertible objects and dictionaries"""

    file_name = "config.json"
    json: ModuleType

    def __init__(self):
        self.json = importlib.import_module("json")

    def serialize(self, writer: ModelStateWriter, obj: Dict):
        with writer.open(self.file_name, "w") as conf:
            self.json.dump(obj, conf)

    def deserialize(self, reader: ModelStateReader) -> Dict:
        with reader.open(self.file_name, "r") as conf:
            return self.json.load(conf)
