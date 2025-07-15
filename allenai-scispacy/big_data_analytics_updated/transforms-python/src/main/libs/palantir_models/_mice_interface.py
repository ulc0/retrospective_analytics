#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
# This file contains the code that should be moved into palantir_models.
# It is temporarily here for testing / development purposes and is copied
# directly into the rust source code.

import importlib
import json
from typing import Any, Dict, Iterable, Iterator, Optional

import models_api.models_api_executable as executable_api
import models_api.models_api_models as models_api
import pandas as pd
import pyarrow as pa
from conjure_python_client import ConjureDecoder
from foundry_ml_live_internal_api.foundry_ml_live_internal_api import (
    InternalModelInputArrowDataframeMetadata,
    InternalModelOutputArrowDataframeMetadata,
)
from models_api import models_api_modelapi as conjure_api

from palantir_models.models import InferenceResults, ModelAdapter, instantiate_adapter
from palantir_models.models._state_accessors import LocalDirectoryModelStateReader
from palantir_models.models.api.user._model_api import ModelApi

from ._non_empty_encoder import NonEmptyConjureEncoder

decoder = ConjureDecoder()
# we use NonEmptyConjureEncoder since arrow metadata cannot have any nulls or None values in it
encoder = NonEmptyConjureEncoder()


class MiceModelInterface:
    """
    Interface used by the MICE image to create and provide inputs to the model.

    The methods here cannot be deleted or modified in any non back-compatible ways, as they are
    called directly by the Rust code.
    """

    @staticmethod
    def instantiate_adapter(
        model_path: str, locator_str: str, container_str: Optional[str] = None, external_str: Optional[str] = None
    ) -> ModelAdapter:
        """
        Instantiates a ModelAdapter using the provided config.
        The model_path holds the model's weights, while the string parameters
        are JSON strings that can be decoded into the appropriate model config objects.
        """
        adapter_locator = decoder.decode(json.loads(locator_str), models_api.PythonLocator)

        container_json = json.loads(container_str) if container_str else None
        external_json = json.loads(external_str) if external_str else None

        container_context = decoder.decode_optional(container_json, executable_api.ContainerizedApplicationContext)
        external_model_context = decoder.decode_optional(external_json, executable_api.ExternalModelExecutionContext)

        adapter_module = importlib.import_module(adapter_locator.module)
        adapter_class = getattr(adapter_module, adapter_locator.name)
        mice_reader = LocalDirectoryModelStateReader(model_path)
        return instantiate_adapter(adapter_class, mice_reader, container_context, external_model_context)

    @staticmethod
    def get_arrow_inputs(input_iter: Iterable[pa.Table]) -> Dict[str, pa.Table]:
        """
        Reads the input tables and returns them as a dictionary of the model input names to arrow tables.
        This is used downstream when calling model.transform().
        """
        input_dict = {}
        for table in input_iter:
            input_dict[_get_input_name_from_metadata(table)] = table
        return input_dict

    @staticmethod
    def get_arrow_outputs(inference_results: InferenceResults) -> Iterable[pa.Table]:
        """
        Reads the inference results and converts them into arrow dataframes with the correct
        metadata headers for the model output names.
        """
        inference_results_dict = inference_results.asdict()
        for name, value in inference_results_dict.items():
            yield _output_to_arrow_table_with_metadata(name, value, None)

    @staticmethod
    def get_arrow_outputs_with_type_casting(
        inference_results: InferenceResults, model_api: ModelApi
    ) -> Iterable[pa.Table]:
        """
        Reads the inference results and converts them into arrow dataframes with the correct
        metadata headers for the model output names. Also casts the model API output types if needed
        """
        inference_results_dict = inference_results.asdict()
        conjure_model_api = model_api.to_service_api()
        for name, value in inference_results_dict.items():
            model_api_output = next(filter(lambda o: o.name == name, conjure_model_api.outputs), None)
            yield _output_to_arrow_table_with_metadata(name, value, model_api_output)

    @staticmethod
    def attach_flamegraph_to_output(output: Iterator[pa.Table], flamegraph: str) -> Iterable[pa.Table]:
        """
        Attaches a flamegraph string to first arrow table in output (iterable) in the metadata conjure object
        """
        first_output_table = next(output)
        parsed_metadata = _get_parsed_metadata_from_pa_table(
            first_output_table, InternalModelOutputArrowDataframeMetadata
        )
        parsed_metadata._flame_graph = flamegraph
        yield first_output_table.replace_schema_metadata(json.loads(encoder.encode(parsed_metadata)))

        for table in output:
            yield table


def _output_to_arrow_table_with_metadata(name, value, model_api_output: Optional[conjure_api.ModelApiOutput]):
    if isinstance(value, pd.DataFrame):
        table = _df_to_arrow_with_type_casting(value, model_api_output)
        new_meta = _get_output_schema_metadata(name)
        return table.replace_schema_metadata(new_meta)

    return _single_value_to_arrow_table(name, value, model_api_output)


def _single_value_to_arrow_table(output_name: str, value, model_api_output: Optional[conjure_api.ModelApiOutput]):
    if model_api_output and model_api_output.type.parameter and model_api_output.type.parameter.type.map:
        data = _dict_to_map_array(value)
    else:
        data = pa.array([value])

    field = pa.field(output_name, data.type)
    schema = pa.schema([field], metadata=_get_output_schema_metadata(output_name))
    return pa.Table.from_arrays([data], schema=schema)


def _get_parsed_metadata_from_pa_table(pyarrow_table, Dataframe):
    # pyarrow encodes to utf bytes which conjure doesn't like
    metadata_as_json = {k.decode(): v.decode() for k, v in pyarrow_table.schema.metadata.items()}
    return decoder.decode(metadata_as_json, Dataframe)


def _get_input_name_from_metadata(pyarrow_table):
    return _get_parsed_metadata_from_pa_table(pyarrow_table, InternalModelInputArrowDataframeMetadata).model_input_name


def _get_output_schema_metadata(output_name):
    metadata_as_json = json.loads(
        encoder.encode(InternalModelOutputArrowDataframeMetadata(model_output_name=output_name))
    )
    return metadata_as_json


def _df_to_arrow_with_type_casting(
    df: pd.DataFrame, model_api_output: Optional[conjure_api.ModelApiOutput]
) -> pa.Table:
    if not model_api_output or not model_api_output.type.tabular:
        return pa.Table.from_pandas(df)

    map_columns = set()
    for column in model_api_output.type.tabular.columns:
        if column.type.map:
            map_columns.add(column.name)

    # Include all columns from df except those that are map columns
    existing_non_map_columns = [col for col in df.columns if col not in map_columns]

    # Create a table from non-map columns
    non_map_df = df[existing_non_map_columns]
    table = pa.Table.from_pandas(non_map_df)

    # Add map columns that exist in the DataFrame
    for map_column in map_columns:
        if map_column in df.columns:
            map_array = _series_to_map_array(df[map_column])
            table = table.append_column(map_column, map_array)

    return table


# Function to convert pandas series of dicts to PyArrow map array
def _series_to_map_array(series):
    keys = []
    values = []
    offsets = [0]

    for d in series:
        keys.extend(d.keys())
        values.extend(d.values())
        offsets.append(len(keys))
    return pa.MapArray.from_arrays(pa.array(offsets), pa.array(keys), pa.array(values))


# Function to convert dict to PyArrow map array
def _dict_to_map_array(d: Dict[Any, Any]):
    keys = list(d.keys())
    values = list(d.values())

    # Recursively convert nested dicts to maps
    values = [_dict_to_map_array(v) if isinstance(v, dict) else v for v in values]

    return pa.MapArray.from_arrays(
        pa.array([0, len(keys)]),  # offsets
        pa.array(keys),
        pa.array(values),
    )
