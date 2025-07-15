#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import uuid
from abc import ABCMeta
from typing import List

from transforms import expectations as E
from transforms.expectations.utils._col_utils import first_failed_value
from transforms.expectations.utils._expectation_utils import check_columns_exist
from transforms.expectations._results import (
    ResultValue,
    ResultType,
    get_conjure_result_value,
    is_not_error_type,
)
from transforms.expectations.evaluator import (
    Results,
    EvaluationTarget,
    JoinEvaluator,
    AggregateEvaluator,
)
from transforms.expectations.wrapper import ExpectationColumn, ExpectationDataFrame


class Expectation(metaclass=ABCMeta):
    def __init__(self):
        self._id = str(uuid.uuid4())

    def id(self):
        return self._id

    def value_column_name(self):
        return f"{self.id()}_value"

    def predicate_column_name(self):
        return f"{self.id()}_predicate"

    def children(self) -> List["Expectation"]:
        return []

    def add_foreign_column(
        self, target: EvaluationTarget, df: ExpectationDataFrame, je: JoinEvaluator
    ) -> ExpectationDataFrame:
        """Override to join in an additional column to the dataframe under evaluation.
        It is not recommended to do this manually, rather you should use the implementation provided
        in the _ForeignValuesExpectation abstract class by subclassing it. A no-op implementation is
        provided here to allow this method to be called on all expectations."""
        return df

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        """Build aggregate columns and queue them to be evaluated later"""
        return

    def columns(self) -> List[str]:
        """Return the columns involved in this expectation."""
        raise NotImplementedError()

    def test_against_all_rows(self) -> bool:
        """Declare if the expectation must be tested against all rows."""
        raise NotImplementedError()

    @check_columns_exist
    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        """Override to extract a special value for the expectation.
        Defaults to relevant columns of the first failed row.

        Columns declared in `columns` method are guaranteed to be present in the dataframe.
        """
        return (
            ResultValue(
                first_failed_value(target.df, predicate_column, *self.columns()),
                ResultType.FAILED_VALUE,
            )
            if self.columns()
            else None
        )

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        """Returns a predicate column for the expectation.

        Predicate should be true for all rows on which the expectation passes."""
        raise NotImplementedError()

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        """Returns an optional value column for the expectation.

        This can be used in predicate and reduce_to_value."""
        raise NotImplementedError()

    def definition(self):
        """Return the expectation as a Data Health Expectation object."""
        raise NotImplementedError()

    def result(self, results: Results) -> any:
        """Returns the result of this expectation as a Data Health ExpectationResult object."""
        result = results[self.id()]

        # We need to special case if the tested dataframe was empty (had 0 rows).
        if result.failed_rows is None:
            status = "PASS" if self.passes_on_empty_dataframe(results) else "FAIL"
        else:
            status = "PASS" if result.failed_rows == 0 else "FAIL"
        return {
            "result": status,
            "value": get_conjure_result_value(result),
            "children": [
                expectation.result(results) for expectation in self.children()
            ],
        }

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        """Defines whether expectation should pass or fail when tested on empty dataframe."""

        return is_not_error_type(results[self.id()])

    def __invert__(self):
        return E.negate(self)

    def __and__(self, other):
        if not isinstance(other, Expectation):
            raise TypeError(
                "You can only use the '&' operator between two expectations."
            )

        return E.all(self, other)

    def __or__(self, other):
        if not isinstance(other, Expectation):
            raise TypeError(
                "You can only use the '|' operator between two expectations."
            )

        return E.any(self, other)
