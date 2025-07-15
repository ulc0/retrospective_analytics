#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
# mypy: disable-error-code="name-defined, attr-defined, return-value"
import importlib
import json
import typing

from palantir_models import ModelSerializer
from palantir_models.models._serialization import ModelStateReader, ModelStateWriter


class XGBoostSerializer(ModelSerializer):
    """
    Simple Serializer for XGBoost Models.

    Default implementation relies of xgboost converting models into/from json.
    A more efficient implementation would use booster.save_raw() for the
    a ubj / bytes version of the model. However, that reduces the compatibility
    guarantees across versions.

    XGBModel is the base class for most / all XBoost model types.
    """

    file_name = "xgboost_model.json"
    metadata_file_name = "metadata.json"

    obj_type_field = "obj_type"
    default_metadata = {"obj_type": "XGBModel"}

    def __init__(self):
        self.xgboost = importlib.import_module("xgboost")
        self.obj_type_map = {
            self.xgboost.Booster: "Booster",
            self.xgboost.sklearn.XGBModel: "XGBModel",
            self.xgboost.sklearn.XGBClassifier: "XGBClassifier",
            self.xgboost.sklearn.XGBRegressor: "XGBRegressor",
            self.xgboost.sklearn.XGBRanker: "XGBRanker",
            self.xgboost.sklearn.XGBRFClassifier: "XGBRFClassifier",
            self.xgboost.sklearn.XGBRFRegressor: "XGBRFRegressor",
        }
        self.inv_obj_type_map = {v: k for k, v in self.obj_type_map.items()}

    def serialize(
        self,
        writer: ModelStateWriter,
        obj: typing.Union["xgboost.sklearn.XGBModel", "xgboost.Booster"],  # noqa: F821
    ):
        metadata = self._get_metadata(obj)
        with writer.open(self.file_name, "w") as xgbfile:
            obj.save_model(xgbfile.name)
        with writer.open(self.metadata_file_name, "w") as metadata_file:
            json.dump(metadata, metadata_file)

    def deserialize(
        self,
        reader: ModelStateReader,
    ) -> typing.Union["xgboost.sklearn.XGBModel", "xgboost.Booster"]:  # noqa: F821
        metadata = self._parse_metadata(reader)
        obj_type_name = metadata.get(self.obj_type_field)
        obj_type = self.inv_obj_type_map.get(obj_type_name, None)

        model = obj_type()
        with reader.open(self.file_name, "r") as xgbfile:
            model.load_model(xgbfile.name)
            return model

    def _get_metadata(self, obj: typing.Union["xgboost.sklearn.XGBModel", "xgboost.Booster"]) -> type:  # noqa: F821
        obj_type_name = None
        for curr_obj_type, curr_name in self.obj_type_map.items():
            # explicitly avoiding isinstance since some classes inherit from XGBModel but their serialization is different
            if type(obj) is curr_obj_type:
                obj_type_name = curr_name

        if obj_type_name is None:
            raise TypeError(
                f"Type {type(obj)} is not one of the compatible XGBoost model types for the XGBoostSerializer: {list(self.obj_type_map.keys())}. Please use either another serializer (e.g. DillSerializer) or create a custom serializer."
            )
        return {self.obj_type_field: obj_type_name}

    def _parse_metadata(self, reader: ModelStateReader) -> type:
        metadata = self.default_metadata
        try:
            with reader.open(self.metadata_file_name, "r") as metadata_file:
                metadata = json.load(metadata_file)
        except FileNotFoundError:
            pass
        return metadata
