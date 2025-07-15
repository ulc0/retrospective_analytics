#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from typing import List

from transforms.expectations import (
    AllExpectation,
    ColumnExpectationFactory,
    GroupByExpectationFactory,
)


class _PrimaryKeyExpectation(AllExpectation):
    def __init__(self, columns: List[str]):
        super(_PrimaryKeyExpectation, self).__init__(
            expectations=list(
                [ColumnExpectationFactory(column).non_null() for column in columns]
                + [GroupByExpectationFactory(columns).is_unique()]
            )
        )
        self._columns = columns

    def definition(self):
        return {
            "type": "primaryKey",
            "primaryKey": {
                "columns": self._columns,
                "all": [expectation.definition() for expectation in self.children()],
            },
        }
