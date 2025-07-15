from pyspark.sql import types as T

from transforms.expectations._results import ResultValue, _append_value
from transforms.expectations._types import (
    column,
    as_comparison_operator,
    timestamp_comparison_value,
    comparison_value_column,
    offset_comparison_value,
)
from transforms.expectations.column._column_expectation import (
    _ComparisonExpectation,
    _ColComparisonExpectation,
)
from transforms.expectations.evaluator import EvaluationTarget
from transforms.expectations.timestamp._timestamp_utils import (
    _to_datetime,
    _to_timedelta,
    _reference_timestamp_now,
)
from transforms.expectations.utils._expectation_utils import (
    check_columns_exist,
    check_and_inject_column_type,
)
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ColumnFunctions as F,
    PolarsOrSparkDataType,
    TIMESTAMP_TYPE,
)


class _TimestampComparisonExpectation(_ComparisonExpectation):
    def __init__(self, col, op, threshold):
        super(_TimestampComparisonExpectation, self).__init__(col, op, threshold)
        self._threshold = _to_datetime(threshold)

    @check_columns_exist
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return super(_TimestampComparisonExpectation, self).reduce_to_value(
            value_column, predicate_column, target
        )

    @check_columns_exist
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    def predicate(  # pylint: disable=arguments-differ
        self,
        value_column: ExpectationColumn,
        target: EvaluationTarget,
        col_type: PolarsOrSparkDataType,
    ) -> ExpectationColumn:
        return (
            F.compare_timestamp(value_column, self._threshold, self._op, col_type)
            | F.col(self._col).isNull()
        )

    @check_columns_exist
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return super(_TimestampComparisonExpectation, self).value(target)

    def definition(self):
        return {
            "type": "timestampComparison",
            "timestampComparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": timestamp_comparison_value(self._threshold),
            },
        }


class _TimestampColumnComparisonExpectation(_ColComparisonExpectation):
    def __init__(self, col, op, other_col, offset_in_seconds):
        super(_TimestampColumnComparisonExpectation, self).__init__(col, op, other_col)
        self._offset_in_seconds = offset_in_seconds

    @check_columns_exist
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._other_col)
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return super(_TimestampColumnComparisonExpectation, self).reduce_to_value(
            value_column, predicate_column, target
        )

    @check_columns_exist
    @check_and_inject_column_type(
        TIMESTAMP_TYPE, lambda self: self._other_col, bind_col_type=False
    )
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    def predicate(  # pylint: disable=arguments-differ
        self, value_column: ExpectationColumn, target: EvaluationTarget, col_type
    ):
        other_col_plus_offset = F.offset_col_by_seconds(
            self._other_col, self._offset_in_seconds
        )

        return F.compare_timestamp(
            F.col(self._col), other_col_plus_offset, self._op, col_type
        ) | (F.col(self._col).isNull() & F.col(self._other_col).isNull())

    @check_columns_exist
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._col)
    @check_and_inject_column_type(TIMESTAMP_TYPE, lambda self: self._other_col)
    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return super(_TimestampColumnComparisonExpectation, self).value(target)

    def definition(self):
        return {
            "type": "timestampComparison",
            "timestampComparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_column(self._other_col),
                "offsetInSeconds": self._offset_in_seconds,
            },
        }

    def id(self):
        return (
            "compare_timestamp_col_"
            + self._op.__name__
            + "$"
            + str(self._col)
            + "$"
            + str(self._other_col)
            + "$"
            + str(self._offset_in_seconds)
        )


class _TimestampRelativeComparisonExpectation(_TimestampComparisonExpectation):
    def __init__(self, col, op, offset):
        self._offset = _to_timedelta(offset)
        self._reference_timestamp = _reference_timestamp_now()
        threshold = self._reference_timestamp + self._offset
        super(_TimestampRelativeComparisonExpectation, self).__init__(
            col, op, threshold
        )

    def result(self, results) -> any:
        super_result = super(_TimestampRelativeComparisonExpectation, self).result(
            results
        )
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
            "type": "timestampComparison",
            "timestampComparison": {
                "column": column(self._col),
                "operator": as_comparison_operator(self._op),
                "threshold": {"type": "now", "now": {}},
                "offsetInSeconds": offset_comparison_value(self._offset),
            },
        }
