# Copyright 2017 Palantir Technologies, Inc.
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from transforms._utils import driver_only

if TYPE_CHECKING:
    import pandas
    from pyspark.sql import DataFrame

    from transforms.foundry.connectors import FoundryConnector

log = logging.getLogger(__name__)

MAX_COLUMN_DESCRIPTION_LENGTH = 800
MAX_TYPECLASS_LENGTH = 100
MAX_TYPECLASS_COUNT = 100


_ColumnTypeClasses = Dict[str, List[Dict[str, str]]]


class DataFrameWriter(object):
    """Object used to write dataframes to an output dataset."""

    _dataframe: Union["DataFrame", None]

    def __init__(
        self,
        foundry: "FoundryConnector",
        rid: str,
        txrid: str,
        branch: str,
        filesystem_id: Optional[str] = None,
        mode: str = "replace",
    ) -> None:
        self._foundry = foundry
        self._rid = rid
        self._txrid = txrid
        self._branch = branch
        self._filesystem_id = filesystem_id or foundry._filesystem_id

        self._mode = mode
        self._dataframe = None
        self._schema_version_id = None

    @property
    def dataframe(self) -> Union["DataFrame", None]:
        return self._dataframe

    @property
    def schema_version_id(self):
        return self._schema_version_id

    @property
    @driver_only
    def _fsm(self):
        return self._foundry._foundry_spark_manager

    @driver_only
    def write(
        self,
        df: "DataFrame",
        partition_cols: Optional[List[str]] = None,
        bucket_cols: Optional[List[str]] = None,
        bucket_count: Optional[int] = None,
        sort_by: Optional[List[str]] = None,
        output_format: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
        column_descriptions: Optional[Dict[str, str]] = None,
        column_typeclasses: Optional[Dict[str, List[Dict[str, str]]]] = None,
    ) -> None:
        """Write the given :class:`~pyspark.sql.DataFrame` to the output datset.

        Args:
            df (pyspark.sql.DataFrame): The PySpark dataframe to write.
            partition_cols (List[str], optional): Column partitioning to use when writing data.
            bucket_cols (List[str], optional): The columns by which to bucket the data.
            bucket_count (int, optional): The number of buckets. Must be specified if bucket_cols or sort_by is given.
            sort_by (List[str], optional): The columns by which to sort the bucketed data.
            format (str, optional): The output file format, defaults to 'parquet'.
            options (dict, optional): Extra options to pass through to
                ``org.apache.spark.sql.DataFrameWriter#option(String, String)
            column_descriptions (Dict[str, str], optional): Mapping of column name keys and description values``.
        """
        if self._dataframe:
            raise ValueError("Cannot write FoundryOutput more than once")
        if bucket_count and bucket_count <= 0:
            raise ValueError("bucket_count must be positive")
        if bucket_cols and not bucket_count:
            raise ValueError("Must specify bucket_count if specifying bucket_cols")
        if sort_by and not bucket_count:
            raise ValueError("Must specify bucket_count if specifying sort_by")

        self._dataframe = df

        new_dataset = self._fsm.write_dataset(
            df,
            self._rid,
            txrid=self._txrid,
            branch=self._branch,
            mode=self._mode,
            filesystem_id=self._filesystem_id,
            partition_cols=partition_cols,
            bucket_cols=bucket_cols,
            bucket_count=bucket_count,
            sort_by=sort_by,
            output_format=output_format,
            options=options,
        )

        self._set_column_metadata(
            df=df,
            column_descriptions=column_descriptions,
            column_typeclasses=column_typeclasses,
        )

        self._schema_version_id = new_dataset.schema_version()

    @driver_only
    def write_pandas(self, pandas_df: "pandas.DataFrame") -> None:
        return self.write(self._foundry.spark_session.createDataFrame(pandas_df))

    @driver_only
    def set_mode(self, mode: str) -> None:
        if mode not in ("replace", "modify", "append"):
            raise ValueError(f"Unknown write mode {mode}")
        self._set_transaction_type(mode)
        self._mode = mode

    @staticmethod
    def _only_valid_column_typeclasses(
        df: "DataFrame",
        column_typeclasses: Optional[_ColumnTypeClasses] = None,
    ) -> _ColumnTypeClasses:
        """Only valid Dict<List<Dict<str, str>>> values are permitted"""
        if not column_typeclasses:
            return {}
        column_typeclasses_intersection = {
            k: column_typeclasses[k] for k in column_typeclasses if k in df.columns
        }
        for column, typeclasses in column_typeclasses_intersection.items():
            # Typeclasses is List<Dict<str, str>>
            if not isinstance(typeclasses, list):
                raise TypeError(
                    f"Column typeclasses for column {column} must be a List<Dict<str, str>>"
                )
            if len(typeclasses) > MAX_TYPECLASS_COUNT:
                raise ValueError(
                    f"Number of typeclasses for column {column} exceeded max {MAX_TYPECLASS_COUNT}"
                )
            for typeclass in typeclasses:
                if not isinstance(typeclass, dict):
                    raise TypeError(
                        f"typeclass list for column {column} expected dict<str, str> "
                        f"instead found {type(typeclass)}"
                    )
                # typeclass is Dict<str, str> where key is one of ['kind', 'name']
                if set(typeclass.keys()) != {"kind", "name"}:
                    raise TypeError(
                        f"Column typeclass for column {column} found unrecognized or incomplete keys. "
                        "Must have both ['kind', 'name']"
                    )
                for typeclass_value in typeclass.values():
                    if not isinstance(typeclass_value, str):
                        raise TypeError(
                            f"Column typeclass value for column {column} must be str"
                        )
                    if len(typeclass_value) > MAX_TYPECLASS_LENGTH:
                        raise ValueError(
                            f"Typeclass value on column {column} exceeded max length {len(typeclass_value)}"
                        )
        return column_typeclasses_intersection

    @staticmethod
    def _only_valid_column_descriptions(
        df: "DataFrame", column_descriptions: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Only valid Dict<str, str> values are permitted"""
        if not column_descriptions:
            return {}
        column_descriptions_intersection = {
            k: column_descriptions[k] for k in column_descriptions if k in df.columns
        }
        for column, description in column_descriptions_intersection.items():
            if not isinstance(description, str):
                raise TypeError(
                    f"Column description for column {column} must be a string"
                )
            if len(description) > MAX_COLUMN_DESCRIPTION_LENGTH:
                raise ValueError(
                    f"Column description for column {column} exceeded max length {MAX_COLUMN_DESCRIPTION_LENGTH}"
                )
        return column_descriptions_intersection

    @staticmethod
    # pylint: disable=dangerous-default-value
    def _prepare_column_metadata(
        valid_column_descriptions: Dict[str, str] = {},
        valid_column_typeclasses: _ColumnTypeClasses = {},
    ) -> Dict[str, Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Compute the valid column metadata dictionary from columns who have both description and typeclass updates,
        those that only have description updates, and those that only have typeclass updates
        """
        valid_column_descriptions_set = set(valid_column_descriptions.keys())
        valid_column_typeclasses_set = set(valid_column_typeclasses.keys())

        # We must post metadata only once so as to avoid overwrite of descriptions and typeclasses on columns.
        # It is also much more efficient to do this post a single time since the network roundtrip may be expensive.
        both_description_and_typeclass_columns = (
            valid_column_descriptions_set.intersection(valid_column_typeclasses_set)
        )
        only_description_columns = valid_column_descriptions_set.difference(
            valid_column_typeclasses_set
        )
        only_typeclass_columns = valid_column_typeclasses_set.difference(
            valid_column_descriptions_set
        )

        # Carefully set the nesting of column: column_metadata for each.
        metadata_to_post = {
            column: {
                "description": valid_column_descriptions[column],
                "typeclasses": valid_column_typeclasses[column],
            }
            for column in both_description_and_typeclass_columns
        }
        metadata_to_post.update(
            {
                column: {"typeclasses": valid_column_typeclasses[column]}
                for column in only_typeclass_columns
            }
        )
        metadata_to_post.update(
            {
                column: {"description": valid_column_descriptions[column]}
                for column in only_description_columns
            }
        )
        return metadata_to_post

    def _set_column_metadata(
        self,
        df: "DataFrame",
        column_descriptions: Optional[Dict[str, str]] = None,
        column_typeclasses: Optional[_ColumnTypeClasses] = None,
    ) -> None:
        """Updates column metadata for descriptions and typeclasses."""
        valid_column_typeclasses = DataFrameWriter._only_valid_column_typeclasses(
            df=df, column_typeclasses=column_typeclasses
        )

        valid_column_descriptions = DataFrameWriter._only_valid_column_descriptions(
            df=df, column_descriptions=column_descriptions
        )

        metadata_to_post = DataFrameWriter._prepare_column_metadata(
            valid_column_descriptions=valid_column_descriptions,
            valid_column_typeclasses=valid_column_typeclasses,
        )

        if len(metadata_to_post.keys()) > 0:
            # Don't accidentally clear metadata when you simply forgot to provide any.
            self._foundry._service_provider.metadata().put_dataset_view_metadata(
                self._foundry.auth_header,
                self._rid,
                self._branch,
                "foundryColumnMetadata",
                metadata_to_post,
                replace=False,
            )

    def _set_transaction_type(self, mode: str) -> None:
        if mode == "replace":
            tx_type = "SNAPSHOT"
        elif mode == "modify":
            tx_type = "UPDATE"
        elif mode == "append":
            tx_type = "APPEND"

        self._foundry._service_provider.catalog().set_transaction_type(
            self._foundry.auth_header, self._rid, self._txrid, tx_type
        )


class RODataFrameWriter(DataFrameWriter):
    @driver_only
    def write(
        self,
        df,
        partition_cols=None,
        bucket_cols=None,
        bucket_count=None,
        sort_by=None,
        output_format="parquet",
        options=None,
        column_descriptions=None,
        column_typeclasses=None,
    ):
        """Write the given :class:`~pyspark.sql.DataFrame` to the output datset.

        Args:
            df (pyspark.sql.DataFrame): The dataframe to write.
        """
        if self._dataframe:
            raise ValueError("Cannot write FoundryOutput more than once")
        self._dataframe = df
        self._dataframe.collect()

    def _set_transaction_type(self, mode: str) -> None:
        pass
