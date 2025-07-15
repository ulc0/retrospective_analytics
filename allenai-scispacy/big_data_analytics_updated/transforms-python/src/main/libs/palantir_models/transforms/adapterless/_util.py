#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

from datetime import date, datetime
from typing import Any, Dict, List

from models_api.models_api_modelapi import ModelApi as ConjureModelApi
from models_api.models_api_modelapi import (
    ModelApiInputTypeVisitor,
    ModelApiOutputTypeVisitor,
    ModelApiTabularFormatVisitor,
    ModelApiValueTypeVisitor,
)

from ...models.api.user._filesystem_io import FileSystemInput, FileSystemOutput
from ...models.api.user._media_io import MediaReferenceInput
from ...models.api.user._model_api import ModelApi
from ...models.api.user._param_io import ParameterInput, ParameterOutput
from ...models.api.user._tabular_io import DFType, ModelApiColumn, TabularInput, TabularOutput


def convert_conjure_model_api_to_internal(source: ConjureModelApi) -> ModelApi:
    def convert_input_output(io_list, visitor_class):
        return [convert_api_type(io, visitor_class) for io in io_list]

    def convert_api_type(api_type, visitor_class):
        return api_type.type.accept(visitor_class(api_type.name, api_type.required))

    def convert_columns(columns):
        return [ModelApiColumn(col.name, col.type.accept(ConjureValueTypeVisitor()), col.required) for col in columns]

    def convert_df_type(format):
        class DFTypeVisitor(ModelApiTabularFormatVisitor):
            def _spark(self, _):
                return DFType.SPARK

            def _pandas(self, _):
                return DFType.PANDAS

        if format:
            return format.accept(DFTypeVisitor())
        else:
            return DFType.SPARK

    class ConjureApiTypeInputVisitor(ModelApiInputTypeVisitor):
        def __init__(self, name, required):
            self.name = name
            self.required = required

        def _tabular(self, tabular):
            columns = convert_columns(tabular.columns)
            df_type = convert_df_type(tabular.format)
            return TabularInput(name=self.name, columns=columns, df_type=df_type, required=self.required)

        def _parameter(self, parameter):
            return ParameterInput(
                name=self.name, type=parameter.type.accept(ConjureValueTypeVisitor()), required=self.required
            )

        def _filesystem(self, _filesystem):
            return FileSystemInput(name=self.name, required=self.required)

        def _media_reference(self, _media_reference):
            return MediaReferenceInput(name=self.name, required=self.required)

    class ConjureApiTypeOutputVisitor(ModelApiOutputTypeVisitor):
        def __init__(self, name, required):
            self.name = name
            self.required = required

        def _tabular(self, tabular):
            columns = convert_columns(tabular.columns)
            df_type = convert_df_type(tabular.format)
            return TabularOutput(name=self.name, columns=columns, df_type=df_type, required=self.required)

        def _filesystem(self, _filesystem):
            return FileSystemOutput(name=self.name, required=self.required)

        def _parameter(self, parameter):
            return ParameterOutput(
                name=self.name, type=parameter.type.accept(ConjureValueTypeVisitor()), required=self.required
            )

    class ConjureValueTypeVisitor(ModelApiValueTypeVisitor):
        def _any(self, _empty):
            return Any

        def _boolean(self, _empty):
            return bool

        def _date(self, _empty):
            return date

        def _double(self, _empty):
            return float

        def _float(self, _empty):
            return float

        def _integer(self, _empty):
            return int

        def _long(self, _empty):
            return int

        def _string(self, _empty):
            return str

        def _timestamp(self, _empty):
            return datetime

        def _media_reference(self, _empty):
            return str

        def _array(self, array_type):
            return List[array_type.value_type.accept(ConjureValueTypeVisitor())]

        def _map(self, map_type):
            key_type = map_type.key_type.accept(ConjureValueTypeVisitor())
            value_type = map_type.value_type.accept(ConjureValueTypeVisitor())
            return Dict[key_type, value_type]

        def _struct(self, struct_type):
            fields = {field.name: field.type.accept(ConjureValueTypeVisitor()) for field in struct_type.fields}
            return fields

    inputs = convert_input_output(source.inputs, ConjureApiTypeInputVisitor)
    outputs = convert_input_output(source.outputs, ConjureApiTypeOutputVisitor)

    return ModelApi(inputs=inputs, outputs=outputs)
