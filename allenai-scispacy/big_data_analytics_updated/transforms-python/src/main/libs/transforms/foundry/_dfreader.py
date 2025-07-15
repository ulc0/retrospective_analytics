# Copyright 2017 Palantir Technologies, Inc.
import logging
from typing import TYPE_CHECKING

from pyspark.sql import types

from transforms._errors import ConjureError, ErrorCode, ErrorType

from .. import _utils
from . import _fsm, _transactions
from ._transactions import has_modified_files, has_removed_files

if TYPE_CHECKING:
    from transforms.foundry.connectors import FoundryConnector

log = logging.getLogger(__name__)


class NonEmptyUnsupportedDataFrame(ConjureError, ValueError):
    """Thrown when trying to read a dataframe that we don't support, but we know it is non-empty.

    Useful for modified and removed views that we currently don't know how to construct, but by
    inspecting the transaction history we are able to figure out if any changes exist that would
    fall into these categories. e.g. DELETE transaction containing files in the 'spark' directory
    means there may be something in the removed view.
    """

    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:NonEmptyUnsupportedDataFrame"
    )


class SchemaNotFoundError(ConjureError, ValueError):
    """Raised when reading a dataframe from a dataset without a schema."""

    error_type = ErrorType(ErrorCode.NOT_FOUND, "TransformsPython:SchemaNotFoundError")


class SchemaMismatchError(ConjureError, ValueError):
    """Raised when the provided schema doesn't match the actual schema of the dataset."""

    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:SchemaMismatchError"
    )


class FoundryResourceDataFrameReader(object):
    def __init__(self, foundry: "FoundryConnector", rid: str) -> None:
        self._foundry = foundry
        self._rid = rid

    @_utils.memoized_property
    def path(self) -> str:
        """str: The Compass path of the resource."""
        return (
            self._foundry._service_provider.compass()
            .get_resource(self._foundry.auth_header, self._rid, ["path"])
            .get("path")
        )

    @property
    @_utils.driver_only
    def _fsm(self):
        return self._foundry._foundry_spark_manager


class TableDataFrameReader(FoundryResourceDataFrameReader):
    def __init__(self, foundry, table_rid, from_version, to_version):
        """Class for reading tables as dataframes."""
        super().__init__(foundry, table_rid)
        self._from_version = from_version
        self._to_version = to_version

    @_utils.driver_only
    def current(self, options=None):
        """Returns a :class:`~pyspark.sql.DataFrame` of the current table view."""
        return self._fsm.read_table(
            rid=self._rid, to_version=self._to_version, additional_options=options
        ).table()

    @_utils.driver_only
    def previous(self, options=None):
        """Returns a :class:`~pyspark.sql.DataFrame` of the previous table view."""
        if self._from_version is None:
            # If no fromVersion is present, the empty dataframe is returned
            return self._fsm.read_table(
                rid=self._rid,
                to_version=self._to_version,
                from_version=self._to_version,
                incremental_mode="added",
                additional_options=options,
            ).table()

        return self._fsm.read_table(
            rid=self._rid, to_version=self._from_version, additional_options=options
        ).table()

    @_utils.driver_only
    def added(self, options=None):
        """Returns a :class:`~pyspark.sql.DataFrame` containing added data."""
        if self._from_version is None:
            # Return a DataFrame containing the entire dataset since all rows are considered unseen.
            return self.current(options=options)

        return self._fsm.read_table(
            rid=self._rid,
            to_version=self._to_version,
            from_version=self._from_version,
            incremental_mode="added",
            additional_options=options,
        ).table()

    @_utils.driver_only
    def modified(self, options=None):
        """Returns a :class:`~pyspark.sql.DataFrame` containing modified data."""
        if self._from_version is None:
            # If no fromVersion is present, the empty dataframe is returned
            return self._fsm.read_table(
                rid=self._rid,
                to_version=self._to_version,
                from_version=self._to_version,
                incremental_mode="added",
                additional_options=options,
            ).table()

        return self._fsm.read_table(
            rid=self._rid,
            to_version=self._to_version,
            from_version=self._from_version,
            incremental_mode="changed",
            additional_options=options,
        ).table()

    @_utils.driver_only
    def removed(self, options=None):
        """Returns a :class:`~pyspark.sql.DataFrame` containing removed data."""
        if self._from_version is None:
            # If no fromVersion is present, the empty dataframe is returned
            return self._fsm.read_table(
                rid=self._rid,
                to_version=self._to_version,
                from_version=self._to_version,
                incremental_mode="added",
                additional_options=options,
            ).table()

        return self._fsm.read_table(
            rid=self._rid,
            to_version=self._to_version,
            from_version=self._from_version,
            incremental_mode="removed",
            additional_options=options,
        ).table()


class DataFrameReader(FoundryResourceDataFrameReader):
    def __init__(self, foundry, dataset_rid, branch, schema_version):
        """Class for reading datasets as dataframes."""
        super().__init__(foundry, dataset_rid)
        self._branch = branch
        self._schema_version = schema_version

    @_utils.memoized_property
    def column_typeclasses(self):
        """Dict<str, List[Dict<str, str>]>: The column typeclasses.  Each typeclass consists of a 'kind' and 'name'.

        Example typeclasses returned from this method are: `[{"kind": "my_kind", "name": "my_name"}]`
        """
        maybe_typeclasses = (
            self._foundry._service_provider.metadata().get_dataset_view_metadata(
                self._foundry.auth_header,
                self._rid,
                self._branch,
                "foundryColumnMetadata",
            )
        )
        if maybe_typeclasses and "foundryColumnMetadata" in maybe_typeclasses:
            return {
                column_name: typeclass_pack["typeclasses"]
                for column_name, typeclass_pack in maybe_typeclasses[
                    "foundryColumnMetadata"
                ].items()
                if "typeclasses" in typeclass_pack
            }
        return {}

    @_utils.memoized_property
    def column_descriptions(self):
        """Dict<str, str>: The column descriptions."""
        maybe_descriptions = (
            self._foundry._service_provider.metadata().get_dataset_view_metadata(
                self._foundry.auth_header,
                self._rid,
                self._branch,
                "foundryColumnMetadata",
            )
        )
        if maybe_descriptions and "foundryColumnMetadata" in maybe_descriptions:
            return {
                column_name: description_pack["description"]
                for column_name, description_pack in maybe_descriptions[
                    "foundryColumnMetadata"
                ].items()
                if "description" in description_pack
            }
        return {}

    @_utils.driver_only
    def current(self, txspec):
        """Returns a :class:`~pyspark.sql.DataFrame` of the current dataset view."""
        start, _prev, end = txspec
        return self._read_dataset(start, end)

    @_utils.driver_only
    def previous(self, txspec, schema=None):
        """Returns a :class:`~pyspark.sql.DataFrame` of the previous dataset view."""
        start, prev, end = txspec

        if not prev:
            # There was no previous time, so empty dataset.
            return self._empty_dataset(end, schema=schema)

        previous_df = self._read_dataset(start, prev)

        # Assert that the schema is the same as the previous schema. This keeps
        # the parameter up to date in case one day we run as a snapshot again.
        if schema and not _utils.data_types_are_equal_ignoring_nullability(
            schema, previous_df.schema
        ):
            raise SchemaMismatchError(
                "The provided schema doesn't match the actual schema of a previous transaction."
                f"Provided: {schema} | Previous: {previous_df.schema}"
            )

        return previous_df

    @_utils.driver_only
    def added(self, txspec):
        """Returns a :class:`~pyspark.sql.DataFrame` containing added data."""
        _start, prev, end = txspec

        if prev and prev == end:
            # Foundry doesn't expose exclusive transaction ranges, so we implement by hand.
            return self._empty_dataset(end)

        if prev:
            return self._read_dataset(
                _transactions.transaction_after(
                    self._foundry, self._rid, prev, end, end
                ),
                end,
            )
        return self.current(txspec)

    @_utils.driver_only
    def modified(self, txspec, schema=None):
        """Returns a :class:`~pyspark.sql.DataFrame` containing modified data."""
        _, previous_end_transaction_rid, end_transaction_rid = txspec
        if has_modified_files(
            self._foundry, self._rid, end_transaction_rid, previous_end_transaction_rid
        ):
            raise NonEmptyUnsupportedDataFrame(
                "Detected modified files since previously processed"
            )
        return self._empty_dataset(end_transaction_rid, schema=schema)

    @_utils.driver_only
    def removed(self, txspec, schema=None):
        """Returns a :class:`~pyspark.sql.DataFrame` containing removed data."""
        _, previous_end_transaction_rid, end_transaction_rid = txspec
        if has_removed_files(
            self._foundry, self._rid, end_transaction_rid, previous_end_transaction_rid
        ):
            raise NonEmptyUnsupportedDataFrame(
                "Detected removed files since previously processed"
            )
        return self._empty_dataset(end_transaction_rid, schema=schema)

    def clear_cache(self) -> None:
        """Clears the dataset cache."""
        self._read_dataset.clear()  # pylint: disable=no-member

    @_utils.memoized_method
    def _empty_dataset(self, end_txrid, schema=None):
        """Construct an empty dataset with the correct schema."""
        versioned_schema = self._foundry._service_provider.schema().get_schema(
            self._foundry.auth_header, self._rid, self._branch, end_txrid=end_txrid
        )

        if versioned_schema:
            # Create a StructType as per https://github.palantir.build/foundry/foundry-spark/blob/ \
            #   422f59b21e504d749212454723e3d033cbaf3b7d/foundry-spark/src/main/java/com/palantir/ \
            #   foundry/spark/FoundrySchemaUtils.java#L52
            fdt = _fsm.FoundryDataTypes()
            pyspark_schema = types.StructType(
                [
                    types.StructField(
                        ffs["name"], fdt.get_spark_data_type(ffs), nullable=True
                    )
                    for ffs in versioned_schema["schema"]["fieldSchemaList"]
                ]
            )

            if schema and schema != pyspark_schema:
                # If a schema is given, then we assert that it is the same as the one we found.
                # This helps prevent bugs when an incremental build one day runs as a snapshot.
                raise SchemaMismatchError(
                    "The schema used to construct an empty dataframe does not match the schema found on the "
                    f"dataset: given {schema}, found {pyspark_schema}"
                )
        else:
            pyspark_schema = schema

        if not pyspark_schema:
            raise SchemaNotFoundError(f"Dataset {self._rid}, transaction {end_txrid}")

        return self._foundry.spark_session.createDataFrame(
            [], schema=schema or pyspark_schema
        ).coalesce(1)

    @_utils.memoized_method
    def _read_dataset(self, start_txrid, end_txrid):
        if not self._foundry.fallback_branches and not self._branch:
            raise SchemaNotFoundError(
                f"Dataset {self._rid}, transaction {end_txrid}, fallback_branches {self._foundry.fallback_branches}, "
                f"branch {self._branch}"
            )

        return self._fsm.read_dataset(
            self._rid,
            branch=self._branch,
            start_txrid=start_txrid,
            end_txrid=end_txrid,
            schema_version=self._schema_version,
        ).dataset()
