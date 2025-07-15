#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.

import operator
from typing import List

from pyspark.sql import types as T

from transforms.expectations.utils._col_utils import first_failed_group
from transforms.expectations.property._column_properties import Aggregate
from transforms.expectations.foreign._dataset_references import (
    DatasetCount,
    _MissingDatasetException,
)
from transforms.expectations.core._expectation import Expectation
from transforms.expectations._types import as_comparison_operator
from transforms.expectations.utils._expectation_utils import check_columns_exist
from transforms.expectations._results import (
    Results,
    ResultValue,
    ResultType,
    is_not_error_type,
)
from transforms.expectations._types import (
    comparison_value_literal,
    comparison_value_cross_dataset,
)
from transforms.expectations.evaluator import (
    EvaluationTarget,
    ParamsToDF,
    AggregateEvaluator,
)
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ColumnFunctions as F,
)


class _RowCountComparisonExpectation(Expectation):
    """Compares the row count of the given groupBy."""

    def __init__(self, cols, op, threshold):
        super(_RowCountComparisonExpectation, self).__init__()
        self._cols = cols
        self._op = op
        self._threshold = threshold
        self._row_count_column_if_no_groups = f"row_count_{self.id()}"

    def test_against_all_rows(self) -> bool:
        return True

    def columns(self) -> List[str]:
        return self._cols

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        if not self._cols:
            ae.queue_aggregate(
                Aggregate(
                    column=F.row_count(),
                    target_column=None,
                    name=self._row_count_column_if_no_groups,
                )
            )

    @check_columns_exist
    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return self._op(value_column, F.lit(self._threshold))

    @check_columns_exist
    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        if self._cols:
            return F.row_count_over_group(self._cols)

        # If there are no group by cols, we use the aggregate column already added.
        return F.col(self._row_count_column_if_no_groups)

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        return ResultValue(
            first_failed_group(
                target.df,
                predicate_column,
                value_column,
                "ROW_COUNT",
                self._cols,
                T.LongType(),
            ),
            ResultType.FAILED_GROUP,
        )

    def definition(self):
        return {
            "type": "rowCount",
            "rowCount": {
                "columns": list(self._cols),
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_literal(self._threshold),
            },
        }

    def passes_on_empty_dataframe(self, _results: Results) -> bool:
        # If empty dataframe, we pass if this expectation passes when tested on 0 row.
        return is_not_error_type(_results[self.id()]) and self._op(0, self._threshold)


class _UniqueExpectation(_RowCountComparisonExpectation):
    """Ensures that the specified columns identify unique rows"""

    def __init__(self, cols):
        super(_UniqueExpectation, self).__init__(cols, operator.le, 1)

    def definition(self):
        return {"type": "unique", "unique": {"columns": list(self._cols)}}


class _DatasetRowCountComparisonExpectation(Expectation):
    def __init__(self, op, threshold: DatasetCount):
        super(_DatasetRowCountComparisonExpectation, self).__init__()
        self._op = op
        self._threshold = threshold
        self._row_count_column = f"row_count_{self.id()}"

    def columns(self) -> List[str]:
        return []

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        ae.queue_aggregate(
            Aggregate(
                column=F.count(F.lit(1)),
                target_column=None,
                name=self._row_count_column,
            )
        )

    def test_against_all_rows(self) -> bool:
        return False

    def _get_other_row_count(self, params_to_df: ParamsToDF):
        try:
            return self._threshold.evaluate(params_to_df)
        except _MissingDatasetException:
            return None

    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        # Since row count will be constant we can just take the first row count we see
        row_count = F.first(value_column)
        other_row_count = self._get_other_row_count(target.params_to_df)

        return ResultValue(
            F.struct(
                row_count.alias("base"), F.lit(other_row_count).alias("threshold")
            ),
            ResultType.CROSS_ROW_COUNT_COMPARISON,
        )

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        other_row_count = self._get_other_row_count(target.params_to_df)
        if other_row_count is None:
            return F.lit(False)

        return self._op(value_column, other_row_count)

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.col(self._row_count_column)

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        result = results[self.id()]
        if not result.value or "threshold" not in result.value:
            return False
        return is_not_error_type(result) and self._op(0, result.value["threshold"])

    def definition(self):
        return {
            "type": "rowCount",
            "rowCount": {
                "columns": [],
                "operator": as_comparison_operator(self._op),
                "threshold": comparison_value_cross_dataset(
                    self._threshold.definition()
                ),
            },
        }
