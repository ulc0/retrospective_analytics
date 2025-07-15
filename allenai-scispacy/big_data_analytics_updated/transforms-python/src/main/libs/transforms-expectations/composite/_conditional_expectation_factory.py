from transforms.expectations.core._expectation import Expectation
from transforms.expectations.composite._composite_expectation import (
    _ConditionalExpectation,
)


class ConditionalExpectationFactory(object):
    """Create expectations dependent on a pre-condition."""

    def __init__(self, when_exp, then_exp):
        self.when_exp = when_exp
        self.then_exp = then_exp

    def otherwise(self, otherwise_exp) -> Expectation:
        return _ConditionalExpectation(self.when_exp, self.then_exp, otherwise_exp)
