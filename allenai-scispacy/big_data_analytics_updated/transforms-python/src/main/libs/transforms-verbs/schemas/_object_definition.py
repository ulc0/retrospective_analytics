#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from collections import namedtuple

from transforms.verbs.schemas._column_definition import COLUMN_FIELDS, ColumnDefinition
from transforms.verbs.schemas._schema_utils import _inflate_if_possible

from transforms import expectations as E
from transforms.verbs.shared._spark_type_parser import _parse_datatype_string


OBJECT_FIELDS = [
    "name",
    "human_name",
    "description",
    "columns",
    "primary_key",
]


class ObjectDefinition(namedtuple("ObjectDefinition", OBJECT_FIELDS)):
    """Definition of a single object in a schema."""

    __slots__ = ()

    def __new__(
        cls,
        name=None,
        human_name=None,
        description=None,
        columns=None,
        primary_key=None,
    ):
        return super(ObjectDefinition, cls).__new__(
            cls, name, human_name, description, columns, primary_key
        )

    FIELDS_TO_OVERRIDE = [
        field for field in COLUMN_FIELDS if field not in ["catalog_column"]
    ]

    def replace_column_references(self, catalog):
        """
        Replace column references from catalog. ValueError if reference is not present in catalog.
        TODO: Add to the SchemaDefinition or the inflation the option to automatically consume catalogs provided
        """
        if catalog is None:
            return
        catalog_columns = catalog.get_as_dict()
        for index, col in enumerate(self.columns):
            if col.catalog_column is not None:
                if col.catalog_column not in catalog_columns:
                    raise ValueError(
                        "No such catalog_column in columns catalog: {0}".format(
                            col.catalog_column
                        )
                    )
                new_column = {}
                referenced_col = catalog_columns[col.catalog_column]
                for field in ObjectDefinition.FIELDS_TO_OVERRIDE:
                    """Replace catalog attributes if override value is present"""
                    override = getattr(col, field)
                    new_column[field] = override or getattr(referenced_col, field)
                self.columns[index] = ColumnDefinition.inflate(new_column)

    def find_column(self, column_name):
        """Finds a column by name.  Error if the column doesn't exist."""
        matches = [col for col in self.columns if col.name == column_name]
        if not matches:
            raise ValueError("No such column: {0}".format(column_name))
        return matches[0]

    def _get_schema_datatypes(self):
        """Converts this object schema dict of dict(colName, dataType)"""
        schema = {}
        for col in self.columns:
            schema[col.name] = _parse_datatype_string(col.data_type)

        return schema

    def get_schema_exact_match_expectation(self):
        """Returns a transforms expectations for an exact schema match"""
        return E.schema().equals(self._get_schema_datatypes())

    def get_contains_schema_expectation(self):
        """
        Returns a transforms expectation for the schema to be contained in the dataframe,
        but allowing for the existence of columns not defined in this object definition exclusive
        """
        return E.schema().contains(self._get_schema_datatypes())

    def get_is_subset_of_schema_expectation(self):
        """Returns a transforms expectation for a subset match of the schema"""
        return E.schema().is_subset_of(self._get_schema_datatypes())

    def get_columns_expectations(self):
        """Converts this object schema dict of dict(colName, dataType)"""
        expectations = []
        for col in self.columns:
            # Skip for columns which does not have expectations
            if col.expectations is None:
                continue

            is_in = col.expectations.is_in
            gte = col.expectations.gte
            lte = col.expectations.lte
            gt = col.expectations.gt
            lt = col.expectations.lt
            if is_in is not None:
                expectations.append(E.col(col.name).is_in(*is_in))
            if gte is not None:
                expectations.append(E.col(col.name).gte(gte))
            if lte is not None:
                expectations.append(E.col(col.name).lte(lte))
            if gt is not None:
                expectations.append(E.col(col.name).gt(gt))
            if lt is not None:
                expectations.append(E.col(col.name).lt(lt))

        return E.all(*expectations)

    @staticmethod
    def inflate(fields):
        """
        Creates an ObjectDefinition from a dictionary of properties.
        Fills in default values where missing.
        """
        # Fill in defaults.
        fields.setdefault("human_name", fields["name"])
        fields.setdefault("description", None)
        fields.setdefault("primary_key", None)

        # Inflate columns if necessary.
        fields["columns"] = [
            _inflate_if_possible(col, ColumnDefinition) for col in fields["columns"]
        ]

        return ObjectDefinition(**fields)
