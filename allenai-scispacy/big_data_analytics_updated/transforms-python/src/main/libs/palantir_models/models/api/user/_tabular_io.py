#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API Tabular Input and Output objects"""

from typing import Any, List, NamedTuple, Optional, Tuple, Type, Union

import models_api.models_api_modelapi as conjure_api
import pandas as pd

from .._types import ApiType, DFType, ModelIO, ServiceContext, UserDefinedApiItem, WritableApiOutput
from .._util import _value_type_to_python_type, convert_to_conjure_value_type


class ModelApiColumn(NamedTuple):
    """
    NamedTuple for Model API Tabular I/O columns
    """

    name: str
    type: type
    required: bool = True

    @classmethod
    def _from_service_api(cls, column: conjure_api.ModelApiColumn):
        return cls(name=column.name, required=column.required, type=_value_type_to_python_type(column.type))


class TabularInput(ApiType):
    """
    An ApiType for foundry tabular inputs
    """

    __columns: List[ModelApiColumn]
    __df_type: DFType

    def __init__(
        self, *, name: str, columns: List[ModelApiColumn], df_type: DFType = DFType.SPARK, required: bool = True
    ):
        super().__init__(name=name, required=required)
        self.__columns = columns
        self.__df_type = df_type

    @property
    def columns(self) -> List[ModelApiColumn]:
        """
        :return: The column schema converted to ModelApiColumn
        """
        return self.__columns

    @property
    def df_type(self):
        """
        :return: The DataFrame type: DFType.PANDAS or DFType.SPARK
        """
        return self.__df_type

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]):
        """
        Binds a TransformIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: TransformIO object
        :return: A spark dataframe or pandas dataframe
        """
        if self.__df_type == DFType.SPARK:
            return io_object.spark_dataframe()
        if self.__df_type == DFType.PANDAS:
            return _cast_model_api_type(io_object.pandas_dataframe(), self.columns)
        # Throw unknown df type
        raise Exception(f"Unexpected ModelInput.Tabular Type: {self.__df_type}")

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        api_columns = _convert_to_conjure_columns(self.__columns)
        api_dftype = _convert_to_conjure_dftype(self.__df_type)
        api_input_type = conjure_api.ModelApiInputType(
            tabular=conjure_api.ModelApiTabularType(columns=api_columns, format=api_dftype)
        )
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.tabular is not None, "must provide a tabular input type"
        name, required, table = service_api_type.name, service_api_type.required, service_api_type.type.tabular
        df_type = DFType.PANDAS if table.format is None or table.format.pandas is not None else DFType.SPARK
        return cls(
            name=name,
            df_type=df_type,
            required=required,
            columns=[ModelApiColumn._from_service_api(col) for col in table.columns],
        )


class TabularOutput(ApiType):
    """
    An ApiType for foundry tabular outputs
    """

    __columns: List[ModelApiColumn]
    __df_type: Optional[DFType]

    def __init__(
        self, *, name: str, df_type: Optional[DFType] = None, columns: List[ModelApiColumn], required: bool = True
    ):
        super().__init__(name=name, required=required)
        self.__df_type = df_type
        self.__columns = columns

    @property
    def columns(self):
        """
        :return: The column schema, list of (name, type)
        """
        return self.__columns  # type: ignore

    @property
    def df_type(self):
        """
        :return: The DataFrame type: DFType.PANDAS or DFType.SPARK
        """
        return self.__df_type

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]) -> WritableApiOutput:
        """
        Binds a TransformIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: TransformIO object
        :return: A WritableTabularOutput object to write to io_object
        """
        return io_object.writable_tabular(self.__df_type)

    def _to_service_api(self) -> conjure_api.ModelApiOutput:
        api_columns = _convert_to_conjure_columns(self.__columns)
        api_dftype = _convert_to_conjure_dftype(self.__df_type)
        api_output_type = conjure_api.ModelApiOutputType(
            tabular=conjure_api.ModelApiTabularType(columns=api_columns, format=api_dftype)
        )
        return conjure_api.ModelApiOutput(name=self.name, required=self.required, type=api_output_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiOutput):
        assert service_api_type.type.tabular is not None, "must provide a tabular output type"
        name, required, table = service_api_type.name, service_api_type.required, service_api_type.type.tabular
        df_type = DFType.PANDAS if table.format is None or table.format.pandas is not None else DFType.SPARK
        return cls(
            name=name,
            df_type=df_type,
            required=required,
            columns=[ModelApiColumn._from_service_api(col) for col in table.columns],
        )


# Column input can be a ModelApiColumn, a column name + type tuple, or just a name (type is assumed to be any)
UserColumnInputType = Union[ModelApiColumn, Tuple[str, Type], str]


class Pandas(UserDefinedApiItem):
    """
    UserDefinedApiItem for a pandas dataframe.
    Is converted to a TabularInput(df_type=DFType.PANDAS) or a TabularOutput.
    """

    __columns: List[ModelApiColumn]

    def __init__(
        self, columns: Optional[List[UserColumnInputType]] = None, name: Optional[str] = None, required: bool = True
    ):
        super().__init__(name, required)
        if columns:
            self.columns = [_convert_to_api_column(col) for col in columns]
        else:
            self.columns = []

    @property
    def columns(self) -> List[ModelApiColumn]:
        """
        :return: The column schema converted to ModelApiColumn
        """
        return self.__columns

    @columns.setter
    def columns(self, columns: List[UserColumnInputType]):
        self.__columns = [_convert_to_api_column(col) for col in columns]

    def _to_input(self) -> TabularInput:
        return TabularInput(name=self.name, columns=self.__columns, df_type=DFType.PANDAS, required=self.required)

    def _to_output(self) -> TabularOutput:
        return TabularOutput(name=self.name, columns=self.__columns, df_type=DFType.PANDAS, required=self.required)


class Spark(UserDefinedApiItem):
    """
    UserDefinedApiItem for a spark dataframe.
    Is converted to a TabularInput(df_type=DFType.SPARK) or a TabularOutput.
    """

    __columns: List[ModelApiColumn]

    def __init__(
        self, columns: Optional[List[UserColumnInputType]] = None, name: Optional[str] = None, required: bool = True
    ):
        super().__init__(name, required)
        if columns:
            self.columns = [_convert_to_api_column(col) for col in columns]
        else:
            self.columns = []

    @property
    def columns(self) -> List[ModelApiColumn]:
        """
        :return: The column schema converted to ModelApiColumn
        """
        return self.__columns

    @columns.setter
    def columns(self, columns: List[UserColumnInputType]):
        self.__columns = [_convert_to_api_column(col) for col in columns]

    def _to_input(self) -> TabularInput:
        return TabularInput(name=self.name, columns=self.__columns, df_type=DFType.SPARK, required=self.required)

    def _to_output(self) -> TabularOutput:
        return TabularOutput(name=self.name, columns=self.__columns, df_type=DFType.SPARK, required=self.required)


def _convert_to_api_column(user_column: Union[ModelApiColumn, Tuple[str, type], str]) -> ModelApiColumn:
    """
    Converts a user column to a ModelApiColumn, or returns the input if it's already a ModelApiColumn.
    """
    # ModelApiColumn is technical an instance of tuple so need to do that check first
    if isinstance(user_column, ModelApiColumn):
        return user_column
    if isinstance(user_column, tuple):
        if len(user_column) != 2:
            raise TypeError(
                "Expected column to be an instance of ModelApiColumn, a tuple of (name, type), or a string, "
                + f"but found tuple of length {len(user_column)}"
            )
        return ModelApiColumn(user_column[0], user_column[1])
    if isinstance(user_column, str):
        return ModelApiColumn(user_column, Any)  # type: ignore
    # Throw unknown user column type
    raise TypeError(
        "Expected column to be an instance of ModelApiColumn, a tuple of (name, type), or a string, "
        + f"but found {type(user_column)}"
    )


def _convert_to_conjure_columns(api_columns) -> List[conjure_api.ModelApiColumn]:
    conjure_columns: List[conjure_api.ModelApiColumn] = []

    for api_column in api_columns:
        api_column_name = api_column.name
        api_column_type = convert_to_conjure_value_type(api_column.type)
        api_column_required = api_column.required
        conjure_columns.append(conjure_api.ModelApiColumn(api_column_name, api_column_required, api_column_type))
    return conjure_columns


def _convert_to_conjure_dftype(df_type: Optional[DFType]) -> Optional[conjure_api.ModelApiTabularFormat]:
    if not df_type:
        return None

    if df_type == DFType.PANDAS:
        return conjure_api.ModelApiTabularFormat(pandas=conjure_api.EmptyTabularFormat())
    if df_type == DFType.SPARK:
        return conjure_api.ModelApiTabularFormat(spark=conjure_api.ModelApiTabularSparkFormat())
    raise ValueError(f"Unexpected DFType: {df_type}")


def _cast_model_api_type(df: pd.DataFrame, model_api_columns: List[ModelApiColumn]) -> pd.DataFrame:
    for column in model_api_columns:
        type_to_check = getattr(column.type, "__origin__", None)
        if column.name in df.columns and type_to_check is dict:
            df[column.name] = df[column.name].apply(_list_of_tuples_to_dict)
    return df


def _list_of_tuples_to_dict(item: Any):
    if isinstance(item, list) and all(isinstance(t, tuple) and len(t) == 2 for t in item):
        return dict(item)
    return item
