#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.

import json
import operator
import re
from abc import ABCMeta
from typing import List

from pyspark.sql import types as T

from transforms.expectations.utils._col_utils import (
    first_failed_value,
    get_actual_column_type,
    get_serialized_conjure_column_type,
)
from transforms.expectations.core._expectation import Expectation
from transforms.expectations.utils._expectation_utils import (
    check_equal_types_compared,
    check_columns_exist,
    static_expectation,
    check_and_inject_column_type,
)
from transforms.expectations._results import (
    ResultValue,
    ResultType,
    Results,
    build_metric_result_value,
    is_not_error_type,
    get_conjure_result_value,
    SCHEMA_RESULT_TYPE_KEY,
    SCHEMA_ERROR_FIELD_KEY,
)
from transforms.expectations._data_types import get_conjure_data_type
from transforms.expectations._types import (
    comparison_value_literal,
    comparison_value_null,
    comparison_value_column,
    column,
    as_comparison_operator,
)
from transforms.expectations.evaluator import EvaluationTarget
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    ColumnFunctions as F,
    ColumnFunctions,
    ARRAY_TYPE,
    ARRAY_OR_MAP_TYPE,
    PolarsOrSparkDataType,
    is_array_type,
)


class _ColumnExpectation(Expectation, metaclass=ABCMeta):
    """Base class for column-based expectations."""

    def __init__(self, col):
        super(_ColumnExpectation, self).__init__()
        self._col = col

    def columns(self) -> List[str]:
        return [self._col]


class _ComparisonExpectation(_ColumnExpectation):
    """Compares the value of a column to the given threshold literal."""

    def __init__(self, col, op, threshold):
        super(_ComparisonExpectation, self).__init__(col)
        self._op = op
        self._threshold = threshold

    def test_against_all_rows(self) -> bool:
        return True

    def _metric(
        self, df: ExpectationDataFrame, metric: ExpectationColumn
    ) -> ExpectationColumn:
        return F.struct(
            F.lit(self._col).alias("column"),
            metric.alias("metric"),
            F.lit(get_serialized_conjure_column_type(df, self._col)).alias("type"),
        )

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        if self._op == operator.gt or self._op == operator.ge:
            return ResultValue(
                build_metric_result_value(target.df, self._col, F.min(value_column)),
                ResultType.MIN_VALUE,
            )
        elif self._op == operator.lt or self._op == operator.le:
            return ResultValue(
                build_metric_result_value(target.df, self._col, F.max(value_column)),
                ResultType.MAX_VALUE,
            )
        return ResultValue(
            first_failed_value(target.df, predicate_column, *self.columns()),
            ResultType.FAILED_VALUE,
        )

    @check_equal_types_compared
    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return (
            self._op(value_column, F.lit(self._threshold)) | F.col(self._col).isNull()
        )

    @check_columns_exist
    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.col(self._col)

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_literal(self._threshold),
            },
        }


@static_expectation
class _ColumnsExistExpectation(Expectation):
    def __init__(self, cols: List[str]):
        super(_ColumnsExistExpectation, self).__init__()
        self._cols = cols

    def test_against_all_rows(self) -> bool:
        return False

    def definition(self):
        return {
            "type": "columnsExist",
            "columnsExist": {
                "columnNames": self._cols,
            },
        }

    def columns(self) -> List[str]:
        return self._cols

    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        errors = [
            self._column_error(col)
            for col in self._cols
            if col not in target.df.columns
        ]
        return ResultValue(
            F.lit(json.dumps(errors)),
            ResultType.SCHEMA_ERROR,
        )

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.lit(all([col in target.df.columns for col in self._cols]))

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        result_value = get_conjure_result_value(results[self.id()])
        if SCHEMA_RESULT_TYPE_KEY not in result_value:
            return True

        schema_result = result_value[SCHEMA_RESULT_TYPE_KEY]
        return (
            not schema_result[SCHEMA_ERROR_FIELD_KEY]
            if SCHEMA_ERROR_FIELD_KEY in schema_result
            else True
        )

    def _column_error(
        self,
        col: str,
    ):
        return {
            "columnName": col,
            "expected": {"type": {"type": "any", "any": {}}},
            "actual": None,
        }


@static_expectation
class _ColumnTypeExpectation(Expectation):
    def __init__(self, col: str, col_type: T.DataType):
        super(_ColumnTypeExpectation, self).__init__()
        self._col = col
        self._col_type = col_type

    def test_against_all_rows(self) -> bool:
        return False

    def definition(self):
        return {
            "type": "columnType",
            "columnType": {
                "columnName": self._col,
                "type": get_conjure_data_type(self._col_type),
            },
        }

    def columns(self) -> List[str]:
        return [self._col]

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        column_type = get_actual_column_type(target.df, self._col)
        return ResultValue(
            F.lit(json.dumps(get_conjure_data_type(column_type))),
            ResultType.COLUMN_TYPE,
        )

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.lit(get_actual_column_type(target.df, self._col) == self._col_type)

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        result = results[self.id()]
        # noinspection PyBroadException
        try:
            # When the dataframe was empty we must manually check if the column type is correct
            value = json.loads(result.value)
            return is_not_error_type(result) and value == get_conjure_data_type(
                self._col_type
            )
        except Exception:
            return False


class _NonNullExpectation(_ColumnExpectation):
    """Ensures the value of a column is not null.

    Comparison to null is undefined therefore we must use built-in isNull/isNotNull functions.
    """

    def __init__(self, col):
        super(_NonNullExpectation, self).__init__(col)

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.col(self._col).isNotNull()

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(operator.ne),
                "threshold": comparison_value_null(),
            },
        }


class _NullExpectation(_ColumnExpectation):
    """Ensures the value of a column is null.
    Comparison to null is undefined therefore we must use built-in isNull/isNotNull functions.
    """

    def __init__(self, col):
        super(_NullExpectation, self).__init__(col)

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.col(self._col).isNull()

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(operator.eq),
                "threshold": comparison_value_null(),
            },
        }


class _ColComparisonExpectation(Expectation):
    """Compares the value of two columns using the given operator."""

    def __init__(self, col, op, other_col):
        super(_ColComparisonExpectation, self).__init__()
        self._col = col
        self._op = op
        self._other_col = other_col

    def test_against_all_rows(self) -> bool:
        return True

    def columns(self) -> List[str]:
        return [self._col, self._other_col]

    def id(self):
        return (
            "compare_col_"
            + self._op.__name__
            + "$"
            + self._col
            + "$"
            + str(self._other_col)
        )

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return self._op(F.col(self._col), F.col(self._other_col)) | (
            F.col(self._col).isNull() & F.col(self._other_col).isNull()
        )

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_column(self._other_col),
            },
        }


class _ColumnRegexExpectation(_ColumnExpectation):
    def __init__(self, col: str, regex: str):
        super(_ColumnRegexExpectation, self).__init__(col)

        if not _ColumnRegexExpectation._is_regex_valid(regex):
            raise ValueError(f"Regex '{regex}' is invalid")

        self._regex = regex

    def test_against_all_rows(self) -> bool:
        return True

    @staticmethod
    def _is_regex_valid(regex):
        try:
            re.compile(regex)
            return True
        except re.error:
            return False

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.col(self._col).isNull() | F.col(self._col).rlike(self._regex)

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "regex",
            "regex": {
                "column": {"type": "name", "name": self._col},
                "regex": self._regex,
            },
        }


class _ColumnValuesExpectation(_ColumnExpectation):
    def __init__(self, col: str, *allowed_values):
        super(_ColumnValuesExpectation, self).__init__(col)
        for value in allowed_values:
            if value is not None and type(value) not in [int, float, str, bool]:
                raise ValueError(
                    "Complex types map/dict/struct are not supported for this expectation."
                )

        self._allowed_values = allowed_values

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        col_type = get_actual_column_type(target.df, self._col)
        if is_array_type(col_type):
            return self._array_predicate(col_type)

        is_in_predicate = ColumnFunctions.isin(F.col(self._col), self._allowed_values)
        if None in self._allowed_values:
            is_in_predicate = is_in_predicate | F.col(self._col).isNull()
        return is_in_predicate

    def _array_predicate(self, col_type: PolarsOrSparkDataType) -> ExpectationColumn:
        return F.array_is_in(self._col, self._allowed_values, col_type)

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    @staticmethod
    def _allowed_value(value):
        if not value:
            return comparison_value_null()

        return comparison_value_literal(value)

    def definition(self):
        return {
            "type": "allowedValues",
            "allowedValues": {
                "column": column(self._col),
                "allowedValues": [
                    _ColumnValuesExpectation._allowed_value(value)
                    for value in self._allowed_values
                ],
            },
        }


class _SizeExpectation(_ColumnExpectation):
    def __init__(self, col, op, threshold):
        super(_SizeExpectation, self).__init__(col)
        self._op = op
        self._threshold = threshold

    def test_against_all_rows(self) -> bool:
        return True

    @check_columns_exist
    @check_and_inject_column_type(ARRAY_OR_MAP_TYPE, lambda self: self._col)
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return super().reduce_to_value(value_column, predicate_column, target)

    @check_columns_exist
    @check_and_inject_column_type(ARRAY_OR_MAP_TYPE, lambda self: self._col)
    def predicate(self, value_column: ExpectationColumn, target: EvaluationTarget):
        return (
            self._op(value_column, F.lit(self._threshold)) | F.col(self._col).isNull()
        )

    @check_columns_exist
    @check_and_inject_column_type(ARRAY_OR_MAP_TYPE, lambda self: self._col)
    def value(  # pylint: disable=arguments-differ
        self, target: EvaluationTarget, col_type: PolarsOrSparkDataType
    ) -> ExpectationColumn:
        return F.size(self._col, col_type)

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": {
                    "type": "expression",
                    "expression": {"type": "size", "size": {"column": self._col}},
                },
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_literal(self._threshold),
            },
        }


class _ArrayContainsExpectation(_ColumnExpectation):
    def __init__(self, col: str, value):
        super(_ArrayContainsExpectation, self).__init__(col)
        self._value = value

    @check_columns_exist
    @check_and_inject_column_type(ARRAY_TYPE, lambda self: self._col)
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return super().reduce_to_value(value_column, predicate_column, target)

    @check_columns_exist
    @check_and_inject_column_type(ARRAY_TYPE, lambda self: self._col)
    def predicate(  # pylint: disable=arguments-differ
        self,
        value_column: ExpectationColumn,
        target: EvaluationTarget,
        col_type: PolarsOrSparkDataType,
    ) -> ExpectationColumn:
        return F.array_contains(F.col(self._col), self._value, col_type)

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "arrayContains",
            "arrayContains": {
                "column": column(self._col),
                "value": {
                    "type": "literal",
                    "literal": {
                        "type": "any",
                        "any": self._value,
                    },
                },
            },
        }

    def test_against_all_rows(self) -> bool:
        return True
