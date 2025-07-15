#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from collections import namedtuple


EXPECTATION_FIELDS = [
    "is_in",
    "gte",
    "lte",
    "gt",
    "lt",
]


class ExpectationDefinition(namedtuple("ExpectationDefinition", EXPECTATION_FIELDS)):
    """Definition of a single expectation for a single column."""

    __slots__ = ()

    @staticmethod
    def inflate(fields):
        """
        Creates a ExpectationDefinition from a dictionary of properties.
        Fills in default values where missing.
        """
        # Fill in defaults.
        fields.setdefault("is_in", None)
        fields.setdefault("gte", None)
        fields.setdefault("lte", None)
        fields.setdefault("gt", None)
        fields.setdefault("lt", None)

        return ExpectationDefinition(**fields)

    def to_string(self):
        description_fields = self.description
        return description_fields
