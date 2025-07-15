#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import json
import logging
from io import BytesIO, IOBase
from typing import Any, Dict, List, NamedTuple, cast

import pyarrow as pa
from conjure_python_client import ConjureDecoder
from foundry_ml_live_internal_api.foundry_ml_live_internal_api import (
    InternalModelInputArrowDataframeMetadata,
    InternalModelOutputArrowDataframeMetadata,
)
from models_api.models_api_modelapi import ModelApi as ConjureModelApi
from models_api.models_api_modelapi import (
    ModelApiFilesystemType,
    ModelApiInputTypeVisitor,
    ModelApiMediaReferenceType,
    ModelApiObjectSetType,
    ModelApiObjectType,
    ModelApiParameterType,
    ModelApiTabularType,
)
from pyspark.sql import DataFrame as SparkDataFrame

from ..._non_empty_encoder import NonEmptyConjureEncoder

log = logging.getLogger(__name__)

ENCODER = NonEmptyConjureEncoder()
DECODER = ConjureDecoder()
SPARK_NOT_SUPPORTED = "Spark input ({}) is not supported for remote inference, please use pandas dataframes."


class ModelInputTypeArrowConverter(ModelApiInputTypeVisitor):
    def __init__(self, input_name: str, input_value: Any):
        self.input_name = input_name
        self.input_value = input_value

    def _get_arrow_input_metadata(self):
        metadata = json.loads(
            ENCODER.encode(InternalModelInputArrowDataframeMetadata(model_input_name=self.input_name))
        )
        return metadata

    def _tabular(self, tabular: ModelApiTabularType):
        schema = pa.Schema.from_pandas(self.input_value)
        table = pa.Table.from_pandas(self.input_value, schema=schema)
        return table.replace_schema_metadata(self._get_arrow_input_metadata())

    def _parameter(self, parameter: ModelApiParameterType):
        data = pa.array([self.input_value])
        field = pa.field(self.input_name, data.type, nullable=True)  # type: ignore  # Default nullable matches Java
        schema = pa.schema([field], metadata=self._get_arrow_input_metadata())
        return pa.Table.from_arrays([data], schema=schema)  # type: ignore

    def _filesystem(self, _filesystem: ModelApiFilesystemType):
        raise RuntimeError("Filesystem input type is not supported with the MICE sidecar.")

    def _media_reference(self, _media_reference: ModelApiMediaReferenceType):
        raise RuntimeError("Media Reference input types are not implemented yet.")

    def _object(self, _object: ModelApiObjectType):
        data = pa.array([self.input_value])
        field = pa.field(self.input_name, data.type, nullable=True)  # type: ignore  # Default nullable matches Java
        schema = pa.schema([field], metadata=self._get_arrow_input_metadata())
        return pa.Table.from_arrays([data], schema=schema)  # type: ignore

    def _object_set(self, _object_set: ModelApiObjectSetType):
        data = pa.array([self.input_value])
        field = pa.field(self.input_name, data.type, nullable=True)  # type: ignore  # Default nullable matches Java
        schema = pa.schema([field], metadata=self._get_arrow_input_metadata())
        return pa.Table.from_arrays([data], schema=schema)  # type: ignore


class ArrowModelInputHandler:
    def __init__(self, model_api: ConjureModelApi):
        self.__model_api = model_api

    def transform_inputs_to_arrow(self, inputs: NamedTuple):
        """
        Converts transform inputs to pyarrow representation and prepares the record batch stream to be sent over the wire.
        Takes in transform-bound NamedTuple.
        """
        api_name_to_input = {input_.name: input_ for input_ in self.__model_api.inputs}

        byte_array_output_stream = BytesIO()

        for input_name, input_value in inputs._asdict().items():
            input_api = api_name_to_input.get(input_name)
            if input_api is None:
                raise ValueError(f"Model input '{input_name}' not found")

            if isinstance(input_value, SparkDataFrame):
                raise ValueError(SPARK_NOT_SUPPORTED.format(input_name))

            handler = ModelInputTypeArrowConverter(input_name, input_value)
            table = input_api.type.accept(handler)

            with pa.RecordBatchStreamWriter(byte_array_output_stream, table.schema) as writer:
                writer.write_table(table)

        byte_array_output_stream.seek(0)
        return byte_array_output_stream


class ArrowModelOutputHandler:
    def __init__(self, model_api: ConjureModelApi):
        self.model_api = model_api

    def get_arrow_outputs(self, response: bytes):
        output_tables = ArrowModelOutputHandler._bytes_to_arrow_table_list(response)
        output_tables_map: Dict[str, Any] = {
            ArrowModelOutputHandler._get_output_name_from_metadata(table): table for table in output_tables
        }

        for api_output in self.model_api.outputs:
            if api_output.type.parameter:
                output_tables_map[api_output.name] = ArrowModelOutputHandler._extract_parameter_output(
                    output_tables_map[api_output.name]
                )

            if api_output.type.tabular:
                tabular_output = api_output.type.tabular
                if tabular_output.format and tabular_output.format.spark:
                    log.warning(f"Output {api_output.name} expected a spark dataframe but received a pandas dataframe.")
                output_tables_map[api_output.name] = output_tables_map[api_output.name].to_pandas()

        return output_tables_map

    @staticmethod
    def _bytes_to_arrow_table_list(input_bytes: bytes) -> List[pa.Table]:
        output_tables = []

        # pa.input_stream needs a memoryview of the bytes
        input_bytes_view = cast(IOBase, memoryview(input_bytes))

        with pa.input_stream(input_bytes_view) as input_stream:
            while True:
                bytes_read = input_stream.tell()
                total_bytes = input_stream.size()

                if bytes_read == total_bytes:
                    break

                new_stream = pa.ipc.open_stream(input_stream)  # type: ignore
                table = new_stream.read_all()
                output_tables.append(table)
        return output_tables

    @staticmethod
    def _get_output_name_from_metadata(pyarrow_table):
        metadata_as_json = {k.decode(): v.decode() for k, v in pyarrow_table.schema.metadata.items()}
        output_metadata = DECODER.decode(metadata_as_json, InternalModelOutputArrowDataframeMetadata)
        return output_metadata.model_output_name

    @staticmethod
    def _extract_parameter_output(pa_table):
        return pa_table.column(0)[0].as_py()
