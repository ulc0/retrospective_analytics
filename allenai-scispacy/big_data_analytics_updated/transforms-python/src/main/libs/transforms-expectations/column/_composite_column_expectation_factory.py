#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.

from transforms.expectations.foreign._dataset_references import DatasetComposite
from transforms.expectations.foreign._foreign_values_expectation import (
    _ReferentialIntegrityExpectation,
)


class CompositeColumnExpectationFactory:
    def __init__(self, cols):
        self._cols = cols

    def matches_foreign_cols(self, foreign_composite: DatasetComposite):
        return _ReferentialIntegrityExpectation(self._cols, foreign_composite)
