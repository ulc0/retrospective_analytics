from pyspark.sql import types as T
from typing import List

from transforms.expectations._results import ResultValue, ResultType, _append_value
from transforms.expectations._types import (
    column,
    as_comparison_operator,
    offset_comparison_value,
)
from transforms.expectations.evaluator import EvaluationTarget, AggregateEvaluator
from transforms.expectations.property._column_properties import ColumnProperty
from transforms.expectations.property._property_expectation import (
    _PropertyComparisonExpectation,
    _TimestampPropertyComparisonExpectation,
)
from transforms.expectations.timestamp._timestamp_utils import (
    _reference_timestamp_now,
    _to_timedelta,
)
from transforms.expectations.utils._col_utils import first_failed_group
from transforms.expectations.utils._expectation_utils import (
    check_columns_exist,
    check_and_inject_column_type,
)
from transforms.expectations.wrapper import PolarsExpectationColumn


class _GroupedPropertyComparisonExpectation(_PropertyComparisonExpectation):
    def __init__(
        self, col, property: ColumnProperty, op, threshold, group_by_cols: List[str]
    ):
        super(_GroupedPropertyComparisonExpectation, self).__init__(
            col, property, op, threshold
        )
        self._group_by_cols = group_by_cols

    def columns(self) -> List[str]:
        return [self._col] + list(self._group_by_cols)

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: PolarsExpectationColumn,
        predicate_column: PolarsExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            first_failed_group(
                target.df,
                predicate_column,
                value_column,
                self._property.name(),
                self._group_by_cols,
                self._property.data_type(),
            ),
            ResultType.FAILED_GROUP,
        )

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        for agg in self._property.aggregates(self._col, self._group_by_cols):
            ae.queue_aggregate(agg)

    @check_columns_exist
    def value(self, target: EvaluationTarget) -> PolarsExpectationColumn:
        return self._property.value(self._col, self._group_by_cols)

    def definition(self):
        return {
            "type": "groupBy",
            "groupBy": {
                "groupByColumns": list(self._group_by_cols),
                "expectation": super().definition(),
            },
        }


class _GroupedPropertyTimestampComparisonExpectation(
    _TimestampPropertyComparisonExpectation
):
    def __init__(
        self, col, property: ColumnProperty, op, threshold, group_by_cols: List[str]
    ):
        super(_GroupedPropertyTimestampComparisonExpectation, self).__init__(
            col, property, op, threshold
        )
        self._group_by_cols = group_by_cols

    def columns(self) -> List[str]:
        return [self._col] + list(self._group_by_cols)

    @check_columns_exist
    @check_and_inject_column_type(T.TimestampType, lambda self: self._col)
    def reduce_to_value(
        self,
        value_column: PolarsExpectationColumn,
        predicate_column: PolarsExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            first_failed_group(
                target.df,
                predicate_column,
                value_column,
                self._property.name(),
                self._group_by_cols,
                self._property.data_type(),
            ),
            ResultType.FAILED_GROUP,
        )

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        for agg in self._property.aggregates(self._col, self._group_by_cols):
            ae.queue_aggregate(agg)

    @check_columns_exist
    @check_and_inject_column_type(T.TimestampType, lambda self: self._col)
    def value(self, target: EvaluationTarget) -> PolarsExpectationColumn:
        return self._property.value(self._col, self._group_by_cols)

    def definition(self):
        return {
            "type": "groupBy",
            "groupBy": {
                "groupByColumns": list(self._group_by_cols),
                "expectation": super().definition(),
            },
        }


class _GroupedPropertyTimestampRelativeComparisonExpectation(
    _GroupedPropertyTimestampComparisonExpectation
):
    def __init__(self, col, property, op, offset, group_by_columns):
        self._offset = _to_timedelta(offset)
        self._reference_timestamp = _reference_timestamp_now()
        threshold = self._reference_timestamp + self._offset
        super(_GroupedPropertyTimestampRelativeComparisonExpectation, self).__init__(
            col, property, op, threshold, group_by_columns
        )

    def result(self, results) -> any:
        super_result = super(
            _GroupedPropertyTimestampRelativeComparisonExpectation, self
        ).result(results)
        now_timestamp_used_value = {
            "type": "timestampUsedAsCurrentTime",
            "timestampUsedAsCurrentTime": {
                "referenceTimestamp": self._reference_timestamp.isoformat(),
            },
        }
        super_result["value"] = _append_value(
            super_result["value"], now_timestamp_used_value
        )
        return super_result

    def definition(self):
        return {
            "type": "groupBy",
            "groupBy": {
                "groupByColumns": list(self._group_by_cols),
                "expectation": {
                    "type": "timestampComparison",
                    "timestampComparison": {
                        "column": column(self._col),
                        "operator": as_comparison_operator(self._op),
                        "threshold": {"type": "now", "now": {}},
                        "offsetInSeconds": offset_comparison_value(self._offset),
                    },
                },
            },
        }
