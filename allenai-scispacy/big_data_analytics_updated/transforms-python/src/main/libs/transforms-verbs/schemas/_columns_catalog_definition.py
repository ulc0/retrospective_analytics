#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from collections import namedtuple

from transforms.verbs.schemas._column_definition import ColumnDefinition
from transforms.verbs.schemas._schema_utils import _inflate_if_possible

COLUMNS_CATALOG_FIELDS = ["columns"]


class ColumnsCatalogDefinition(
    namedtuple("ColumnsCatalogDefinition", COLUMNS_CATALOG_FIELDS)
):
    """Definition of a catalog of columns in a single object."""

    __slots__ = ()

    def get_as_dict(self):
        columns_map = {}
        for reference in self.columns:
            columns_map[reference.name] = reference
        return columns_map

    @staticmethod
    def inflate(fields):
        """
        Creates a ColumnsCatalogDefinition from a dictionary of properties.
        """
        fields["columns"] = [
            _inflate_if_possible(col, ColumnDefinition) for col in fields["columns"]
        ]
        return ColumnsCatalogDefinition(**fields)
