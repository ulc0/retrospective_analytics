#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import json
from enum import Enum
from typing import Any, NamedTuple, Dict, List, Union

from transforms.expectations.utils._col_utils import get_serialized_conjure_column_type
from transforms.expectations.wrapper import (
    ExpectationColumn,
    value_col_is_valid,
    ColumnFunctions as F,
)

FAILED_ROWS_KEY = "failed_rows"
VALUE_KEY = "value"
VALUE_TYPE_KEY = "value_type"

SCHEMA_RESULT_TYPE_KEY = "schemaError"
SCHEMA_ERROR_FIELD_KEY = "errors"


class ResultType(Enum):
    MAX_VALUE = "maxValue"
    MIN_VALUE = "minValue"
    COLUMN_TYPE = "columnType"
    NUMERIC_PROPERTY_METRIC = "numericPropertyMetric"
    CROSS_ROW_COUNT_COMPARISON = "crossRowCountComparison"
    FAILED_VALUE = "failedValue"
    FAILED_GROUP = "failedGroup"
    SCHEMA_ERROR = "schemaError"


class ErrorType(Enum):
    MISSING_COLUMNS = "missingColumns"
    INVALID_COLUMN_TYPE = "invalidColumnType"
    FOREIGN_DATASET_ERROR = "foreignDatasetError"
    EXCEPTION = "exception"


ValueType = Union[ResultType, ErrorType]


class ResultValue(NamedTuple):
    """Wrapper used when reducing an expectation to a result value."""

    column: ExpectationColumn
    type: ValueType


class CollectedResultValue(NamedTuple):
    """Object containing the result value of an expectation after it's been applied to a dataframe."""

    failed_rows: Union[int, None]
    value: Any = None
    type: Union[ValueType, None] = None


def is_not_error_type(result: CollectedResultValue):
    if isinstance(result.type, ErrorType) and result.value is not None:
        return False

    return True


class Results(Dict[str, CollectedResultValue]):
    def __init__(
        self, results: Dict[str, CollectedResultValue], exception: Exception = None
    ):
        """Opaque object representing the results of an evaluation of one or more expectations.

        This object should be passed back to the `Expectation.result()` method to render the result for each
        expectation.
        """
        super(Results, self).__init__(results)
        self.exception = exception


def build_result_value(result: Dict) -> CollectedResultValue:
    value_type = None

    if present_and_not_none(result, VALUE_TYPE_KEY):
        if result[VALUE_TYPE_KEY] in ResultType.__members__:
            value_type = ResultType[result[VALUE_TYPE_KEY]]
        else:
            value_type = ErrorType[result[VALUE_TYPE_KEY]]

    return CollectedResultValue(
        failed_rows=(
            result[FAILED_ROWS_KEY]
            if present_and_not_none(result, FAILED_ROWS_KEY)
            else None
        ),
        value=result[VALUE_KEY] if value_col_is_valid(result) else None,
        type=value_type,
    )


def present_and_not_none(result, key) -> bool:
    return key in result and result[key] is not None


def build_results_struct(
    failed_rows_column: ExpectationColumn, result_value: ResultValue = None
) -> ExpectationColumn:
    value_column = result_value.column if result_value else F.lit(None)
    value_type = F.lit(result_value.type.name) if result_value else F.lit(None)
    return F.struct(
        failed_rows_column.alias(FAILED_ROWS_KEY),
        value_column.alias(VALUE_KEY),
        value_type.alias(VALUE_TYPE_KEY),
    )


def build_metric_result_value(df, col, metric: ExpectationColumn) -> ExpectationColumn:
    return F.struct(
        F.lit(col).alias("column"),
        metric.alias("metric"),
        F.lit(get_serialized_conjure_column_type(df, col)).alias("type"),
    )


def missing_columns_result(missing_columns: List[str]) -> ResultValue:
    return ResultValue(
        F.array(*[F.lit(col) for col in missing_columns]), ErrorType.MISSING_COLUMNS
    )


def get_conjure_result_value(result: CollectedResultValue):
    if not result.type and not result.value:
        return _failed_row_count(result)

    if result.type == ResultType.COLUMN_TYPE:
        return {"type": "columnType", "columnType": json.loads(result.value)}
    elif result.type == ErrorType.MISSING_COLUMNS:
        return {"type": "missingColumns", "missingColumns": {"columns": result.value}}
    elif result.type == ErrorType.INVALID_COLUMN_TYPE:
        return {
            "type": "invalidColumnType",
            "invalidColumnType": json.loads(result.value),
        }
    elif result.type == ResultType.MAX_VALUE:
        return _metric(result, "max")
    elif result.type == ResultType.MIN_VALUE:
        return _metric(result, "min")
    elif result.type == ResultType.NUMERIC_PROPERTY_METRIC:
        return _numeric_property_metric(result)
    elif result.type == ResultType.SCHEMA_ERROR:
        return (
            None
            if result.failed_rows == 0
            else {
                "type": SCHEMA_RESULT_TYPE_KEY,
                SCHEMA_RESULT_TYPE_KEY: {
                    SCHEMA_ERROR_FIELD_KEY: json.loads(result.value)
                },
            }
        )
    elif result.type == ResultType.FAILED_VALUE and result.value:
        return _with_failed_row_count(result, _failed_row(result))
    elif result.type == ResultType.FAILED_GROUP:
        return _with_failed_row_count(result, _failed_group(result))
    elif result.type == ResultType.CROSS_ROW_COUNT_COMPARISON:
        return {
            "type": "rowCountComparison",
            "rowCountComparison": {
                "base": (
                    0
                    if not result.value or "base" not in result.value
                    else result.value["base"]
                ),
                "threshold": (
                    None
                    if not result.value or "threshold" not in result.value
                    else result.value["threshold"]
                ),
            },
        }
    elif result.type == ErrorType.FOREIGN_DATASET_ERROR:
        if result.value:
            if "err" in result.value:
                return json.loads(result.value["err"])
            elif "failed_val" in result.value:
                # return result.value['failed_val']
                return _with_failed_row_count(
                    result, _failed_row_from_value(result.value["failed_val"])
                )
    elif result.type == ErrorType.EXCEPTION:
        return {"type": "exception", "exception": {}}
    return _failed_row_count(result)


def _failed_row(result):
    return {
        "type": "failedRow",
        "failedRow": {
            "cells": [
                _cell_from_struct(name, cell) for name, cell in result.value.items()
            ]
        },
    }


def _failed_row_from_value(value):
    return {
        "type": "failedRow",
        "failedRow": {
            "cells": [_cell_from_struct(name, cell) for name, cell in value.items()]
        },
    }


def _failed_group(result):
    if result.value is None:
        return None
    elif result.value["value"] is None:
        return None

    result_value = result.value["value"]
    return {
        "type": "failedGroup",
        "failedGroup": {
            "groupCells": [
                _cell_from_struct(name, cell)
                for name, cell in result_value["group_by_cells"].items()
                # polars always forces schema on empty structs, so {} becomes {'literal': None}
                if cell is not None
            ],
            "valueCells": [
                _cell_from_struct(
                    result.value["value_name"], result_value["value_cell"]
                ),
            ],
        },
    }


def _with_failed_row_count(result: CollectedResultValue, extra_value: Dict):
    if extra_value is None:
        if result.failed_rows is None:
            return None

        return _failed_row_count(result)

    if result.failed_rows is None:
        return extra_value

    return {"type": "many", "many": [_failed_row_count(result), extra_value]}


def _failed_row_count(result: CollectedResultValue):
    return (
        None
        if not result.failed_rows
        else {"type": "rowCount", "rowCount": {"count": result.failed_rows}}
    )


def _numeric_property_metric(result):
    value = (
        result.value["value"]
        if "value" in result.value and result.value["value"]
        else 0
    )
    return {
        "type": "metric",
        "metric": {
            "type": "numericProperty",
            "numericProperty": {
                "columnName": result.value["columnName"],
                "value": value,
                "type": result.value["type"],
            },
        },
    }


def _metric(result: CollectedResultValue, metric_type: str):
    metric_value = (
        result.value["metric"]
        if "metric" in result.value and result.value["metric"]
        else None
    )
    metric = {
        "type": "metric",
        "metric": {
            "type": metric_type,
            metric_type: _cell(
                result.value["column"], metric_value, result.value["type"]
            ),
        },
    }
    return _with_failed_row_count(result, metric) if result.failed_rows else metric


def _append_value(current_value, new_value):
    if current_value is None:
        return new_value
    elif current_value["type"] == "many":
        # Just add the new result to the current list
        current_value["many"].append(new_value)
        return current_value
    else:
        # Wrap everything in a many
        value = {"type": "many", "many": [current_value, new_value]}
        return value


def _cell_from_struct(name, cell_struct):
    return _cell(
        name,
        cell_struct["value"] if "value" in cell_struct else None,
        cell_struct["type"] if "type" in cell_struct else None,
    )


def _cell(name, value, data_type):
    return {
        "name": name,
        "value": value,
        "type": json.loads(data_type) if data_type else None,
    }
