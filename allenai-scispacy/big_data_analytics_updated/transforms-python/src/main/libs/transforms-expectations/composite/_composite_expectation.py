from abc import ABCMeta
from typing import List

from transforms.expectations.core._expectation import Expectation
from transforms.expectations.utils._expectation_utils import static_expectation
from transforms.expectations._results import ResultValue, ResultType, Results
from transforms.expectations.utils._col_utils import missing_columns, first_failed_value
from transforms.expectations.evaluator import (
    AggregateEvaluator,
    EvaluationTarget,
    JoinEvaluator,
)
from transforms.expectations.wrapper import (
    ExpectationColumn,
    ExpectationDataFrame,
    ColumnFunctions as F,
)


class _CompositeExpectation(Expectation, metaclass=ABCMeta):
    def __init__(self, expectations: List[Expectation]):
        super(_CompositeExpectation, self).__init__()
        self._expectations = expectations

    def add_aggregate_columns(self, ae: AggregateEvaluator):
        for expectation in self._expectations:
            expectation.add_aggregate_columns(ae)

    def add_foreign_column(
        self, target: EvaluationTarget, df: ExpectationDataFrame, je: JoinEvaluator
    ) -> ExpectationDataFrame:
        for expectation in self._expectations:
            df = expectation.add_foreign_column(target, df, je)
        return df

    def test_against_all_rows(self) -> bool:
        return any(child.test_against_all_rows() for child in self._expectations)

    def columns(self) -> List[str]:
        involved_columns = []
        for child_expectation in self._expectations:
            for column in child_expectation.columns():
                if column not in involved_columns:
                    involved_columns.append(column)
        return involved_columns

    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        # We override the top level reduce_to_value in this case because
        # we just want to filter out missing columns from the failed row example, not fail on them.
        missing_cols = missing_columns(self.columns(), target.df)
        columns = [col for col in self.columns() if col not in missing_cols]
        first_failed_value_col = first_failed_value(
            target.df, predicate_column, *columns
        )
        return (
            ResultValue(
                first_failed_value_col,
                ResultType.FAILED_VALUE,
            )
            if self.columns()
            else None
        )

    def children(self):
        return self._expectations


@static_expectation
class AllExpectation(_CompositeExpectation):
    def __init__(self, expectations: List[Expectation]):
        super(AllExpectation, self).__init__(expectations)

    def reduce_to_value(
        self,
        value_column: ExpectationColumn,
        predicate_column: ExpectationColumn,
        target: EvaluationTarget,
    ) -> ResultValue:
        # We do not collect results for all expectation because the child results will tell the full story.
        # The values we'd get in an all expectation are rather misleading. E.g. a failed schema expectation inside might
        # lead us to show a failed row that doesn't have any real failed values.
        pass

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        predicate = F.lit(True)
        for exp in self._expectations:
            predicate = predicate & exp.predicate(exp.value(target), target)
        return predicate

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "all",
            "all": {
                "of": [expectation.definition() for expectation in self._expectations]
            },
        }

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        return all(
            child.passes_on_empty_dataframe(results) for child in self.children()
        )


@static_expectation
class AnyExpectation(_CompositeExpectation):
    def __init__(self, expectations: List[Expectation]):
        super(AnyExpectation, self).__init__(expectations)

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        predicate = F.lit(False)
        for exp in self._expectations:
            predicate = predicate | exp.predicate(
                F.col(exp.value_column_name()), target
            )
        return predicate

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "any",
            "any": {
                "of": [expectation.definition() for expectation in self._expectations]
            },
        }

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        return any(
            child.passes_on_empty_dataframe(results) for child in self.children()
        )

    def result(self, results: Results) -> any:
        # For an any expectation, failing children don't really have any meaning.
        # In case of E.any(a, b) both a and b might be failing but the top level expectation could be passing given
        # there may be no rows where both a AND b fails.
        return _mark_failing_sub_results_not_applicable(super().result(results))


@static_expectation
class NotExpectation(_CompositeExpectation):
    def __init__(self, expectation_to_negate: Expectation):
        super(NotExpectation, self).__init__([expectation_to_negate])
        self._expectation_to_negate = expectation_to_negate

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return ~self._expectation_to_negate.predicate(
            F.col(self._expectation_to_negate.value_column_name()), target
        )

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "not",
            "not": {"expectation": self._expectation_to_negate.definition()},
        }


class _ConditionalExpectation(_CompositeExpectation, metaclass=ABCMeta):
    """
    Assert that rows passing the when-expectation also pass the then-expectation.
    Rows that do not pass the when-expectation must pass the otherwise-expectation.
    """

    def __init__(
        self, when_exp: Expectation, then_exp: Expectation, otherwise: Expectation
    ):
        super(_ConditionalExpectation, self).__init__([when_exp, then_exp, otherwise])
        self.when_exp = when_exp
        self.then_exp = then_exp
        self.otherwise = otherwise

    def test_against_all_rows(self) -> bool:
        return (
            self.when_exp.test_against_all_rows()
            or self.then_exp.test_against_all_rows()
            or self.otherwise.test_against_all_rows()
        )

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.when(
            self.when_exp.predicate(F.col(self.when_exp.value_column_name()), target),
            self.then_exp.predicate(F.col(self.then_exp.value_column_name()), target),
        ).otherwise(
            self.otherwise.predicate(F.col(self.otherwise.value_column_name()), target)
        )

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        return {
            "type": "conditional",
            "conditional": {
                "when": self.when_exp.definition(),
                "then": self.then_exp.definition(),
                "otherwise": self.otherwise.definition(),
            },
        }

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        if self.when_exp.passes_on_empty_dataframe(results):
            return self.then_exp.passes_on_empty_dataframe(results)
        else:
            return self.otherwise.passes_on_empty_dataframe(results)


def _mark_failing_sub_results_not_applicable(result):
    return {
        "result": result["result"],
        "value": result["value"],
        "children": [
            {
                "result": "NOT_APPLICABLE" if child["result"] == "FAIL" else "PASS",
                "value": child["value"],
                "children": child["children"],
            }
            for child in result["children"]
        ],
    }
