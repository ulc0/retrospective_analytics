#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
from typing import Optional

import mio_api.mio as mio_api
import pandas as pd
import pyarrow as pa
import pyspark.sql

from palantir_models.models.api._types import (
    _VALID_PARAMETER_TYPES,
    DFType,
    ModelIO,
    ObjectPrimaryKey,
    ObjectSetRid,
    UnassignedOutput,
    WritableParameterOutput,
    WritableTabularOutput,
    _ParameterType,
)
from palantir_models.models.api.user._media_io import ModelsMediaReference


class PandasIO(ModelIO):
    """
    Implementation of ModelIO for Pandas Dataframes
    """

    _CLASSES_TO_REGISTER = (pd.DataFrame,)
    __delegate: pd.DataFrame

    def __init__(self, pandas_df):
        self.__delegate = pandas_df
        self._delegate_class_name = self.__delegate.__class__.__name__

    def pandas_dataframe(self):
        return self.__delegate


class SparkIO(ModelIO):
    """
    Implementation of ModelIO for Spark Dataframes
    """

    _CLASSES_TO_REGISTER = (pyspark.sql.DataFrame,)
    __delegate: pyspark.sql.DataFrame

    def __init__(self, spark_df):
        self.__delegate = spark_df
        self._delegate_class_name = self.__delegate.__class__.__name__

    def spark_dataframe(self):
        return self.__delegate


class PyArrowIO(ModelIO):
    """
    Implementation of ModelIO for PyArrow Tables
    """

    _CLASSES_TO_REGISTER = (pa.Table,)
    __delegate: pa.Table

    def __init__(self, pyarrow_table):
        self.__delegate = pyarrow_table
        self._delegate_class_name = self.__delegate.__class__.__name__

    def pandas_dataframe(self):
        return self.__delegate.to_pandas()

    def parameter(self):
        return self.__delegate.column(0)[0].as_py()

    def object(self):
        return ObjectPrimaryKey(str(self.__delegate.column(0)[0].as_py()))

    def object_set(self):
        return ObjectSetRid(str(self.__delegate.column(0)[0].as_py()))


class UnassignedOutputIO(ModelIO):
    """
    Implementation of ModelIO used to initialize WritableApiOutput
    objects not assigned to foundry output objects.
    """

    _CLASSES_TO_REGISTER = (UnassignedOutput,)

    def __init__(self, __delegate):
        pass

    def writable_tabular(self, df_type: Optional[DFType] = None):
        return WritableTabularOutput(df_type)

    def writable_parameter(self, param_type: type, required: bool):
        return WritableParameterOutput(param_type, required)

    def writable_filesystem(self):
        raise TypeError(
            "Filesystem outputs are not supported for ModelAdapter.transform(). "
            "Please use transform_write() instead."
        )


class ParameterIO(ModelIO):
    """
    Implementation of ModelIO for parameter types
    """

    _CLASSES_TO_REGISTER = _VALID_PARAMETER_TYPES
    __delegate: _ParameterType

    def __init__(self, param_io):
        self.__delegate = param_io
        self._delegate_class_name = self.__delegate.__class__.__name__

    def parameter(self) -> _ParameterType:
        return self.__delegate

    def media_reference(
        self,
        binary_media_set_service: mio_api.BinaryMediaSetService,
        media_set_service: mio_api.MediaSetService,
        auth_header: str,
    ):
        return ModelsMediaReference(
            self.__delegate,  # type: ignore
            binary_media_set_service,
            media_set_service,
            auth_header,
        )

    def object(self):
        if isinstance(self.__delegate, str):
            return ObjectPrimaryKey(self.__delegate)
        else:
            raise ValueError(f"Cannot represent type {type(self.__delegate)} as an object.")

    def object_set(self):
        if isinstance(self.__delegate, str):
            return ObjectSetRid(self.__delegate)
        else:
            raise ValueError(f"Cannot represent type {type(self.__delegate)} as an object set.")


class ObjectIO(ModelIO):
    """
    Implementation of ModelIO for OSDK object types
    """

    _CLASSES_TO_REGISTER = (ObjectPrimaryKey,)
    __delegate: ObjectPrimaryKey

    def __init__(self, object_io):
        self.__delegate = object_io
        self._delegate_class_name = self.__delegate.__class__.__name__

    def object(self):
        return self.__delegate


class ObjectSetIO(ModelIO):
    """
    Implementation of ModelIO for OSDK object types
    """

    _CLASSES_TO_REGISTER = (ObjectSetRid,)
    __delegate: ObjectSetRid

    def __init__(self, object_set_io):
        self.__delegate = object_set_io
        self._delegate_class_name = self.__delegate.__class__.__name__

    def object_set(self):
        return self.__delegate
