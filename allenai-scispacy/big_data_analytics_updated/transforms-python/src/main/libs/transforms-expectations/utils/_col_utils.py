#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import json
from typing import List, Union

from transforms.expectations._data_types import get_conjure_data_type
from transforms.expectations.wrapper import (
    ExpectationColumn,
    PolarsOrSparkDataType,
    ExpectationDataFrame,
    PolarsOrSparkSchemaField,
    PolarsOrSparkDataType,
    ColumnFunctions as F,
)


def missing_columns(cols: List[str], df: ExpectationDataFrame) -> List[str]:
    """Returns all columns not present in the dataframe."""
    return [col for col in cols if col not in df.columns]


def get_field(
    df: ExpectationDataFrame, col: str
) -> Union[PolarsOrSparkSchemaField, None]:
    try:
        return df.schema[col]
    except KeyError:
        return None


def get_actual_column_type(
    df: ExpectationDataFrame, col: str
) -> Union[PolarsOrSparkDataType, None]:
    field = get_field(df, col)
    return field.dataType if field else None


def get_serialized_conjure_column_type(
    df: ExpectationDataFrame, col: str
) -> Union[str, None]:
    field = get_field(df, col)
    return _serialize_data_type(field.dataType) if field else None


def _serialize_data_type(data_type: PolarsOrSparkDataType) -> str:
    return json.dumps(get_conjure_data_type(data_type))


def first_failed_group(
    df: ExpectationDataFrame,
    predicate_column: ExpectationColumn,
    value_column: ExpectationColumn,
    value_name: str,
    group_by_cols: List[str],
    data_type: PolarsOrSparkDataType,
):
    """Extract the first failed group as a Struct with relevant information."""
    group_by_cols_as_cells = [
        _get_cell_struct(df, col).alias(col) for col in group_by_cols
    ]
    cells_column = F.struct(
        F.struct(*group_by_cols_as_cells).alias("group_by_cells"),
        _get_cell_struct_with_type(value_column, data_type).alias("value_cell"),
    )
    first_failed_value = _first_failed_value(predicate_column, cells_column).alias(
        "value"
    )

    return F.when(first_failed_value.isNull(), F.lit(None)).otherwise(
        F.struct(
            first_failed_value,
            F.lit(value_name).alias("value_name"),
        )
    )


def first_failed_value(
    df: ExpectationDataFrame, predicate_column: ExpectationColumn, *cols: str
) -> ExpectationColumn:
    """Returns the first failed value as a Struct with a column for each col in cols."""
    value_column = F.struct(*[_get_cell_struct(df, col).alias(col) for col in cols])
    return _first_failed_value(predicate_column, value_column)


def _get_cell_struct(df: ExpectationDataFrame, col: str):
    return F.struct(
        F.col(col).alias("value"),
        F.lit(get_serialized_conjure_column_type(df, col)).alias("type"),
    )


def _get_composite_cell_struct(df: ExpectationDataFrame, cols: List[str]):
    return F.struct(
        F.array(*cols).alias("value"),
        F.lit(
            "["
            + ",".join([get_serialized_conjure_column_type(df, col) for col in cols])
            + "]"
        ).alias("type"),
    )


def _get_cell_struct_with_type(
    value_column: ExpectationColumn, data_type: PolarsOrSparkDataType
) -> ExpectationColumn:
    return F.struct(
        value_column.alias("value"),
        F.lit(_serialize_data_type(data_type)).alias("type"),
    )


def _first_failed_value(
    predicate_column: ExpectationColumn, value_column: ExpectationColumn
) -> ExpectationColumn:
    first_failed_value = F.first_non_null(
        F.when(predicate_column, None).otherwise(value_column),
        ignorenulls=True,
    )
    return first_failed_value
