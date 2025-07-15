#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Implementations of the ModelIO protocol for various transforms I/O types"""

from contextlib import contextmanager
from io import IOBase
from typing import Iterator, Optional, Union

import pandas as pd
import pyspark
from transforms.api import FileSystem, LightweightInput, LightweightOutput, TransformInput, TransformOutput

from palantir_models import DFType
from palantir_models.models.api import ModelIO, WritableFileSystemOutput, WritableTabularOutput


class TransformIO(ModelIO):
    """
    Implementation of ModelIO for Transform I/O
    """

    _CLASSES_TO_REGISTER = (TransformInput, TransformOutput)
    __delegate: Union[TransformInput, TransformOutput]

    def __init__(self, transform_io):
        self.__delegate = transform_io
        self._delegate_class_name = self.__delegate.__class__.__name__

    def filesystem(self):
        return self.__delegate.filesystem()

    def pandas_dataframe(self):
        return self.__delegate.pandas()

    def spark_dataframe(self):
        return self.__delegate.dataframe()

    def writable_tabular(self, df_type: Optional[DFType] = None):
        return WritableTransformsTabularOutput(self.__delegate, df_type)

    def writable_filesystem(self):
        return WritableTransformsFileSystemOutput(self.__delegate.filesystem())


class TransformsFileSystemIO(ModelIO):
    """
    Implementation of ModelIO for TransformIO Filesystems
    """

    _CLASSES_TO_REGISTER = (FileSystem,)
    __delegate: FileSystem

    def __init__(self, transforms_fs):
        self.__delegate = transforms_fs
        self._delegate_class_name = self.__delegate.__class__.__name__

    def filesystem(self):
        return self.__delegate

    def writable_filesystem(self):
        return WritableTransformsFileSystemOutput(self.__delegate)


class LightweightIO(ModelIO):
    """
    Implementation of ModelIO for Lightweight Transform I/O
    """

    _CLASSES_TO_REGISTER = (LightweightInput, LightweightOutput)
    __delegate: Union[LightweightInput, LightweightOutput]

    def __init__(self, lightweight_io):
        self.__delegate = lightweight_io
        self._delegate_class_name = self.__delegate.__class__.__name__

    def filesystem(self):
        return self.__delegate.filesystem()

    def pandas_dataframe(self):
        return self.__delegate.pandas()

    def writable_tabular(self, df_type=None):
        return WritableLightweightTabularOutput(self.__delegate, df_type)


class WritableTransformsTabularOutput(WritableTabularOutput):
    """
    Class for writing to foundry datasets.
    Accepts spark or pandas dataframe as input.
    :param tabular_output: The foundry dataset to write to.
    """

    __tabular_output: Optional[TransformOutput]

    def __init__(self, tabular_output: Optional[TransformOutput], df_type: Optional[DFType] = None):
        super().__init__(df_type)
        self.__tabular_output = tabular_output

    def _perform_write(self):
        if isinstance(self._inference_result, pyspark.sql.DataFrame):
            self.__tabular_output.write_dataframe(self._inference_result)
        elif isinstance(self._inference_result, pd.DataFrame):
            self.__tabular_output.write_pandas(self._inference_result)


class WritableTransformsFileSystemOutput(WritableFileSystemOutput):
    """
    Class for writing to foundry dataset file systems.
    Writing occurs directly via `open` during run_inference.
    :param transform_output: The foundry dataset to write to.
    """

    __output_fs: FileSystem

    def __init__(self, transforms_output_fs: FileSystem):
        self.__output_fs = transforms_output_fs

    @contextmanager
    def open(self, asset_file_path: str, mode: str = "wb") -> Iterator[IOBase]:
        """
        Open a file in this writer's
        output filesystem for writing.
        """
        with self.__output_fs.open(asset_file_path, mode) as backing_file:
            yield backing_file

    def _perform_write(self):
        # Writing is performed during open() instead.
        pass


class WritableLightweightTabularOutput(WritableTabularOutput):
    """
    Class for writing tabular LightweightOutputs to foundry datasets.

    Accepts as input one of:
    - Pandas DataFrame
    - Arrow Table
    - Polars DataFrame
    - LazyFrame

    :param tabular_output: The foundry dataset to write to.
    """

    __tabular_output: Optional[LightweightOutput]

    def __init__(self, tabular_output: Optional[LightweightOutput], df_type: Optional[DFType] = None):
        super().__init__(df_type)
        self.__tabular_output = tabular_output

    def _perform_write(self):
        self.__tabular_output.write_table(self._inference_result)
