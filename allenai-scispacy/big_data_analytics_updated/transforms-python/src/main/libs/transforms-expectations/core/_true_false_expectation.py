from abc import ABCMeta

from typing import List

from transforms.expectations import Expectation
from transforms.expectations.utils._expectation_utils import static_expectation
from transforms.expectations._results import Results
from transforms.expectations.evaluator import EvaluationTarget
from transforms.expectations.wrapper import ExpectationColumn, ColumnFunctions as F


@static_expectation
class TrueFalseExpectation(Expectation, metaclass=ABCMeta):
    def __init__(self, value: bool):
        super(TrueFalseExpectation, self).__init__()
        self._value = value

    def columns(self) -> List[str]:
        """Return the columns involved in this expectation."""
        return []

    def test_against_all_rows(self) -> bool:
        """Declare if the expectation must be tested against all rows."""
        return False

    def value(self, target: EvaluationTarget) -> ExpectationColumn:
        return F.lit(None)

    def definition(self):
        if self._value:
            return {"type": "passing", "passing": {}}
        else:
            return {"type": "failing", "failing": {}}

    def predicate(
        self, value_column: ExpectationColumn, target: EvaluationTarget
    ) -> ExpectationColumn:
        return F.lit(self._value)

    def passes_on_empty_dataframe(self, results: Results) -> bool:
        return self._value
