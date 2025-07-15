from abc import ABC
from collections import OrderedDict
from io import BytesIO
from typing import Any, NamedTuple

import pandas as pd
import pyarrow as pa
from pyspark.sql import DataFrame as SparkDataFrame

from ...models.api import TabularInput
from ._arrow_util import SPARK_NOT_SUPPORTED, ArrowModelInputHandler, ArrowModelOutputHandler
from ._remote_adapter import ArrowRemoteAdapter


class ChunkingInferenceNotSupportedException(Exception):
    """
    Exception raised when attempting to run inference with chunking on invalid input
    """


class ArrowRemoteChunkingAdapter(ArrowRemoteAdapter, ABC):
    def __init__(self, *args, use_chunking, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_chunking = use_chunking

    """
    Subclass of ArrowRemoteAdapter that adds optional chunking support.
    """

    MAX_CHUNK_BYTES = 25 * 1024 * 1024

    def run_inference(self, inputs: NamedTuple, outputs: NamedTuple) -> None:
        if not self.use_chunking:
            return super().run_inference(inputs, outputs)

        self._run_chunked_inference(inputs, outputs)

    def _run_chunked_inference(self, inputs, outputs) -> None:
        dataframe_inputs = []
        other_inputs = OrderedDict()
        api_inputs = {io.name: io for io in self._api().inputs}

        for input_name, input_value in inputs._asdict().items():
            if isinstance(api_inputs[input_name], TabularInput):
                dataframe_inputs.append((input_name, input_value))
            else:
                other_inputs[input_name] = input_value

        if len(dataframe_inputs) != 1:
            error_msg = (
                "Chunking is supported only if there is exactly one dataframe input. "
                f"Found {len(dataframe_inputs)} dataframe input(s)."
            )
            raise ChunkingInferenceNotSupportedException(error_msg)

        df_name, df_value = dataframe_inputs.pop()
        if isinstance(df_value, SparkDataFrame):
            raise ValueError(SPARK_NOT_SUPPORTED.format(df_name))

        big_table = pa.Table.from_pandas(df_value)
        accumulated_outputs: dict[str, Any] = {}
        for out_def in self._conjure_model_api.outputs:
            accumulated_outputs[out_def.name] = []

        num_rows = big_table.num_rows
        start_idx = 0
        chunk_size = self._compute_max_rows_for_chunk(big_table)
        while start_idx < num_rows:
            end_idx = min(start_idx + chunk_size, num_rows)
            chunk_table = big_table.slice(start_idx, chunk_size)
            chunk_df = chunk_table.to_pandas()
            chunked_inputs_dict = other_inputs.copy()
            chunked_inputs_dict[df_name] = chunk_df

            inputs_chunked, outputs_chunked = self._api().bind_args(chunked_inputs_dict, context=self._service_context)
            arrow_bytes = ArrowModelInputHandler(self._conjure_model_api).transform_inputs_to_arrow(inputs_chunked)
            results = self._internal_arrow_transform(arrow_bytes)
            output_tables = ArrowModelOutputHandler(self._conjure_model_api).get_arrow_outputs(results)

            for key, val in output_tables.items():
                accumulated_outputs[key].append(val)

            start_idx = end_idx

        # Now write or merge all chunked results into the final outputs
        for key, parted_tables in accumulated_outputs.items():
            writable_value = getattr(outputs, key)

            if parted_tables and isinstance(parted_tables[0], pd.DataFrame):
                final_df = pd.concat(parted_tables, ignore_index=True)
                writable_value.write(final_df)
            elif parted_tables:
                # For parameter or scalar, fall back to the last chunk
                writable_value.write(parted_tables[-1])

    def _compute_max_rows_for_chunk(self, table: pa.Table) -> int:
        """
        Computes how many rows can fit in a single chunk to keep the arrow bytes under MAX_CHUNK_BYTES.
        A simple heuristic is to sample a subset of rows for size, or rely on row_size ~ total_size/num_rows.
        """
        total_size = table.nbytes
        total_rows = table.num_rows
        avg_row_size = total_size / total_rows if total_rows else 1
        max_rows = int(self.MAX_CHUNK_BYTES // avg_row_size)
        return max_rows

    def _input_arrow_bytes_hook(self, arrow_bytes: BytesIO) -> BytesIO:
        if len(arrow_bytes.getvalue()) > self.MAX_CHUNK_BYTES:
            raise ValueError(
                "The model input exceeds the transport limit. "
                "To enable chunking, set model.use_chunking=True. "
                "Chunking is available for inputs with exactly one DataFrame "
                "and any number of parameters."
            )
        return arrow_bytes
