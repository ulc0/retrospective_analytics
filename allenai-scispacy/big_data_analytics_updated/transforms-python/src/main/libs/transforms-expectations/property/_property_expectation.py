from pyspark.sql import types as T
from typing import List

from transforms.expectations._results import (
    ResultValue,
    ResultType,
    Results,
    _append_value,
    build_metric_result_value,
    is_not_error_type,
)
from transforms.expectations._types import (
    as_comparison_operator,
    comparison_value_literal,
    timestamp_comparison_value,
    offset_comparison_value,
)
from transforms.expectations._types import column
from transforms.expectations.core._expectation import Expectation
from transforms.expectations.evaluator import EvaluationTarget, AggregateEvaluator
from transforms.expectations.property._column_properties import (
    ColumnProperty,
    MaxProperty,
)
from transforms.expectations.timestamp._timestamp_utils import (
    _to_datetime,
    _reference_timestamp,
    _to_timedelta,
    _reference_timestamp_now,
)
from transforms.expectations.utils._expectation_utils import (
    check_columns_exist,
    check_and_inject_column_type,
)
from transforms.expectations.wrapper import (
    PolarsExpectationColumn,
    ColumnFunctions as F,
)


class _PropertyComparisonExpectation(Expectation):
    """Compares the value of a column to the given threshold literal."""

    def __init__(self, col, property: ColumnProperty, op, threshold):
        super(_PropertyComparisonExpectation, self).__init__()
        self._col = col
        self._property = property
        self._op = op
        self._threshold = threshold

    def test_against_all_rows(self) -> bool:
        return True

    def columns(self) -> List[str]:
        return [self._col]

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: PolarsExpectationColumn,
        predicate_column: PolarsExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            F.struct(
                F.lit(self._col).alias("columnName"),
                F.lit(self._property.name()).alias("type"),
                # Since the property values are constant over the dataframes we can pick any of them.
                F.first(value_column).alias("value"),
            ),
            ResultType.NUMERIC_PROPERTY_METRIC,
        )

    @check_columns_exist
    def predicate(
        self, value_column: PolarsExpectationColumn, target: EvaluationTarget
    ) -> PolarsExpectationColumn:
        return self._op(value_column, F.lit(self._threshold))

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        for agg in self._property.aggregates(self._col, []):
            ae.queue_aggregate(agg)

    @check_columns_exist
    def value(self, target: EvaluationTarget) -> PolarsExpectationColumn:
        return self._property.value(self._col, [])

    def _get_col_definition(self):
        return {
            "type": "property",
            "property": {
                "columnName": self._col,
                "propertyType": self._property.name(),
            },
        }

    def definition(self):
        return {
            "type": "comparison",
            "comparison": {
                "column": self._get_col_definition(),
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_literal(self._threshold),
            },
        }

    def passes_on_empty_dataframe(self, _results: Results) -> bool:
        # We consider column properties = 0 on empty dataframes
        return is_not_error_type(_results[self.id()]) and self._op(0, self._threshold)


class _TimestampPropertyComparisonExpectation(_PropertyComparisonExpectation):
    def __init__(self, col, property: ColumnProperty, op, threshold):
        super(_TimestampPropertyComparisonExpectation, self).__init__(
            col, property, op, threshold
        )
        self._threshold = _to_datetime(threshold)

    @check_columns_exist
    @check_and_inject_column_type(T.TimestampType, lambda self: self._col)
    def reduce_to_value(
        self,
        value_column: PolarsExpectationColumn,
        predicate_column: PolarsExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        result_type = (
            ResultType.MAX_VALUE
            if self._property.name() == MaxProperty().name()
            else ResultType.MIN_VALUE
        )

        return ResultValue(
            build_metric_result_value(target.df, self._col, F.first(value_column)),
            result_type,
        )

    @check_columns_exist
    @check_and_inject_column_type(T.TimestampType, lambda self: self._col)
    def predicate(
        self, value_column: PolarsExpectationColumn, target: EvaluationTarget
    ) -> PolarsExpectationColumn:
        return self._op(value_column, self._threshold) | F.col(self._col).isNull()

    @check_columns_exist
    @check_and_inject_column_type(T.TimestampType, lambda self: self._col)
    def value(self, target: EvaluationTarget) -> PolarsExpectationColumn:
        return super(_TimestampPropertyComparisonExpectation, self).value(target)

    def definition(self):
        return {
            "type": "timestampComparison",
            "timestampComparison": {
                "column": self._get_col_definition(),
                "operator": as_comparison_operator(self._op),
                "threshold": timestamp_comparison_value(self._threshold),
            },
        }

    def passes_on_empty_dataframe(self, _results: Results) -> bool:
        # We consider column properties to have 0 value
        return is_not_error_type(_results[self.id()]) and self._op(
            _reference_timestamp(), self._threshold
        )


class _TimestampPropertyRelativeComparison(_TimestampPropertyComparisonExpectation):
    def __init__(self, col, property, op, offset):
        self._offset = _to_timedelta(offset)
        self._reference_timestamp = _reference_timestamp_now()
        threshold = self._reference_timestamp + self._offset
        super(_TimestampPropertyRelativeComparison, self).__init__(
            col, property, op, threshold
        )

    def result(self, results) -> any:
        super_result = super(_TimestampPropertyRelativeComparison, self).result(results)
        now_timestamp_used_value = {
            "timestampUsedAsCurrentTime": {
                "referenceTimestamp": self._reference_timestamp.isoformat(),
            },
            "type": "timestampUsedAsCurrentTime",
        }
        super_result["value"] = _append_value(
            super_result["value"], now_timestamp_used_value
        )
        return super_result

    def definition(self):
        return {
            "type": "timestampComparison",
            "timestampComparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": {"type": "now", "now": {}},
                "offsetInSeconds": offset_comparison_value(self._offset),
            },
        }
