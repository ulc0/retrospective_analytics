#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from collections import namedtuple
from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.verbs.schemas._object_definition import ObjectDefinition
from transforms.verbs.schemas._schema_utils import _inflate_if_possible

SCHEMA_FIELDS = [
    "name",
    "human_name",
    "description",
    "objects",
]


class SchemaDefinition(namedtuple("SchemaDefinition", SCHEMA_FIELDS)):
    """Definition of an dataset schemas."""

    __slots__ = ()

    def __new__(
        cls,
        name=None,
        human_name=None,
        description=None,
        objects=None,
        external_checks=None,
        external_mappings=None,
    ):
        return super(SchemaDefinition, cls).__new__(
            cls, name, human_name, description, objects
        )

    def __init__(self, *args, **kwargs):
        super(SchemaDefinition, self).__init__()
        for obj in self.objects:
            self.objects[self.objects.index(obj)] = ObjectDefinition(
                name=obj.name,
                human_name=obj.human_name,
                description=obj.description,
                primary_key=obj.primary_key,
                columns=obj.columns,
            )

    def find_object(self, object_name):
        """Finds an object by name.  Error if the object doesn't exist."""
        matches = [obj for obj in self.objects if obj.name == object_name]
        if not matches:
            raise ValueError("No such object: {0}".format(object_name))
        return matches[0]

    def force_compliance(self, df, object_name):
        """
        Forces an object to be compliant with the schema by dropping columns that
        shouldn't be there, and filling others with null
        """
        base_object = self.find_object(object_name)
        target_cols = base_object.columns
        visited_columns = set([])
        existing_cols = set(df.columns)

        projection = []
        for column in target_cols:
            column_name = column.name
            if column_name in visited_columns:
                continue
            if (
                column_name not in existing_cols
                or not self._column_type_matches_schema(df, column_name, target_cols)
            ):
                projection.append(F.lit(None).cast(column.data_type).alias(column_name))
            else:
                projection.append(F.col(column_name))
            visited_columns.add(column_name)

        df = df.select(*projection)
        return df

    @staticmethod
    def inflate(fields):
        """
        Creates a SchemaDefinition from a dictionary of properties.
        Fills in default values where missing.
        """
        # Fill in defaults.
        fields.setdefault("human_name", fields["name"])
        fields.setdefault("description", None)

        # Inflate columns if necessary.
        fields["objects"] = [
            _inflate_if_possible(obj, ObjectDefinition) for obj in fields["objects"]
        ]

        return SchemaDefinition(**fields)

    @staticmethod
    def _column_type_matches_schema(df, col_name, columns):
        """
        Checks if the type of the given col_name in the given df matches the type of the column in the given
        list of schema columns.

        We do this by casting the column to the desired type, and then checking that the casted type matches the
        original type. This is necessary because Spark allows us to specify different names when casting to what it
        returns in dtypes. The main example being that if we cast to 'long', the resulting dtype is 'bigint'.
        Doing the check in this way means our code doesn't need specific knowledge of these naming differences.
        """
        column_to_check_list = [col for col in columns if col.name == col_name]

        # Return false if the column is not found at all.
        if len(column_to_check_list) == 0:
            return False

        column_to_check = column_to_check_list[0]

        actual_data_type = df.schema[col_name].dataType
        expected_data_type = T._parse_datatype_string(column_to_check.data_type)

        return actual_data_type == expected_data_type
