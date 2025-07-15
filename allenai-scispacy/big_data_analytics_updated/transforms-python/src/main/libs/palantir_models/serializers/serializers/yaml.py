#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import importlib
from types import ModuleType
from typing import Dict

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class YamlSerializer(ModelSerializer[Dict]):
    """Serializer for yaml-convertible objects and dictionaries"""

    file_name = "config.yaml"
    yaml: ModuleType

    def __init__(self):
        self.yaml = importlib.import_module("yaml")

    def serialize(self, writer: ModelStateWriter, obj: Dict):
        with writer.open(self.file_name, "w") as conf:
            self.yaml.safe_dump(obj, conf)

    def deserialize(self, reader: ModelStateReader) -> Dict:
        with reader.open(self.file_name, "r") as conf:
            return self.yaml.safe_load(conf)
