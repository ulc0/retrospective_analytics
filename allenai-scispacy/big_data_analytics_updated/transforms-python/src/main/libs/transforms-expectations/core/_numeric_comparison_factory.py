import operator


class NumericComparisonFactory(object):
    def __init__(self, expectation_factory):
        self.numeric_expectation_factory = expectation_factory

    def gt(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.gt, threshold)

    def gte(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.ge, threshold)

    def lt(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.lt, threshold)

    def lte(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.le, threshold)

    def equals(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.eq, threshold)

    def not_equals(self, threshold) -> "Expectation":
        return self.numeric_expectation_factory(operator.ne, threshold)
