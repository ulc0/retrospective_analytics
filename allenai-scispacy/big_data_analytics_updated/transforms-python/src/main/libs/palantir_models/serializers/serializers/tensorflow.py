#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
# mypy: disable-error-code="name-defined"
import enum
import importlib
import os
from types import ModuleType
from typing import Any, Dict, Optional

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter


class TensorflowFormat(enum.Enum):
    DIR = 0
    H5 = 1
    KERAS = 2

    def get_save_path(self, dir_path):
        if self == TensorflowFormat.DIR:
            return dir_path
        if self == TensorflowFormat.H5:
            return os.path.join(dir_path, "model.h5")
        if self == TensorflowFormat.KERAS:
            return os.path.join(dir_path, "model.keras")


class TensorflowKerasSerializer(ModelSerializer):
    """Serializer for tensorflow keras models"""

    DIR_NAME: str = "tensorflow_saved_model_dir"
    __tensorflow: ModuleType

    def __init__(self, format=TensorflowFormat.DIR, custom_objects: Optional[Dict[str, Any]] = None):
        self.__tensorflow = importlib.import_module("tensorflow")
        self.__custom_objects = custom_objects
        self.__format = format

    def serialize(
        self,
        writer: ModelStateWriter,
        obj: "tensorflow.keras.Model",  # noqa: F821
    ):
        dir_path = writer.mkdir(self.DIR_NAME)
        self._save_model(dir_path, obj)

    def deserialize(self, reader: ModelStateReader) -> "tensorflow.keras.Model":  # noqa: F821
        dir_path = reader.dir(self.DIR_NAME)
        obj = self._load_model(dir_path)
        obj.compile()
        return obj

    def _save_model(self, dir_path: str, obj: "tensorflow.keras.Model"):  # noqa: F821
        try:
            save_path = self.__format.get_save_path(dir_path)
            try:
                obj.save(save_path)
            except ValueError as exc:
                if (
                    self.__format == TensorflowFormat.DIR
                ):  # Later TF versions have lost support for saving/loading in this way. Try keras format.
                    save_path = TensorflowFormat.KERAS.get_save_path(dir_path)
                    obj.save(save_path)
                else:
                    raise exc
        except ValueError as exc:
            if "Invalid filepath extension for saving" in str(exc):
                raise ValueError(
                    "Serialization failed due to invalid save format. Please specify the correct file format in the TensorflowKerasSerializer constructor:\n"
                    "\tTensorflowKerasSerializer(format=TensorflowFormat.DIR) to use directory saving. This save format is considered legacy in Tensorflow\n"
                    "\tTensorflowKerasSerializer(format=TensorflowFormat.H5) to use .h5 file format saving\n"
                    "\tTensorflowKerasSerializer(format=TensorflowFormat.KERAS) to use .keras file format saving\n"
                ) from exc
            raise exc

    def _load_model(self, dir_path: str) -> "tensorflow.keras.Model":  # noqa: F821
        save_path = self.__format.get_save_path(dir_path)
        try:
            return self.__tensorflow.keras.models.load_model(
                save_path, custom_objects=self.__custom_objects, compile=False
            )
        except ValueError as exc:
            if (
                self.__format == TensorflowFormat.DIR
            ):  # Later TF versions have lost support for saving/loading in this way. Try keras format.
                save_path = TensorflowFormat.KERAS.get_save_path(dir_path)
                return self.__tensorflow.keras.models.load_model(
                    save_path, custom_objects=self.__custom_objects, compile=False
                )
            else:
                raise ValueError(
                    "Deserialization failed. If you are receiving this error, check that the tensorflow version being used is the same as when the model was saved."
                ) from exc
