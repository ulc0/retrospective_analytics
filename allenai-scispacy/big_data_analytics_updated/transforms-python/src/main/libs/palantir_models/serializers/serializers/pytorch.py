#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import importlib
from types import ModuleType

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class PytorchStateSerializer(ModelSerializer):
    """Serializer for PyTorch state dictionaries."""

    STATE_DICT_FILE_NAME = "model_state_dict.pt"
    torch: ModuleType

    def __init__(self):
        self.torch = importlib.import_module("torch")

    def serialize(self, writer: ModelStateWriter, obj: dict):
        """Serializes the state_dict of a PyTorch model."""
        with writer.open(self.STATE_DICT_FILE_NAME, "wb") as file_path:
            self.torch.save(obj, file_path)

    def deserialize(self, reader: ModelStateReader) -> dict:
        """Deserializes the state_dict of a PyTorch model."""
        with reader.open(self.STATE_DICT_FILE_NAME, "rb") as file_path:
            state_dict = self.torch.load(file_path)
            return state_dict
