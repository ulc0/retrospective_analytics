#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from collections import namedtuple

from transforms.verbs.schemas._expectation_definition import (
    EXPECTATION_FIELDS,
    ExpectationDefinition,
)
from transforms.verbs.schemas._schema_utils import _inflate_if_possible


COLUMN_FIELDS = [
    "name",
    "human_name",
    "data_type",
    "example",
    "description",
    "required",
    "catalog_column",
    "expectations",
]


class ColumnDefinition(namedtuple("ColumnDefinition", COLUMN_FIELDS)):
    """Definition of a single column in a single object."""

    __slots__ = ()

    @staticmethod
    def inflate(fields):
        """
        Creates a ColumnDefinition from a dictionary of properties.
        Fills in default values where missing.
        """
        # Fill in defaults.
        fields.setdefault("name", None)
        fields.setdefault("human_name", fields["name"])
        fields.setdefault("data_type", None)
        fields.setdefault("example", None)
        fields.setdefault("description", None)
        fields.setdefault("required", False)
        fields.setdefault("catalog_column", None)
        fields.setdefault("expectations", None)

        # Inflate expectations if necessary.
        if fields["expectations"] is not None:
            fields["expectations"] = _inflate_if_possible(
                fields["expectations"], ExpectationDefinition
            )

        if fields["name"] is not None and fields["catalog_column"] is not None:
            raise ValueError('Only one of ["name", "catalog_column"] can be defined')
        if fields["name"] is None and fields["catalog_column"] is None:
            raise ValueError('Either "name" or "catalog_column" must be defined')

        return ColumnDefinition(**fields)

    def to_string(self):
        description_fields = self.description
        return description_fields
