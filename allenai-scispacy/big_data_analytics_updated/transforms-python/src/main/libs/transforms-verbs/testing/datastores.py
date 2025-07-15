#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
import logging
import errno
import os
import re
import json
from contextlib import contextmanager
from pathlib import PurePath
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from transforms.api import FileStatus


log = logging.getLogger(__name__)

WRITE_MODES = set(["w", "wb", "w+", "wb+"])


class CompoundDatastore(object):
    # pylint: disable=invalid-name,no-member
    """Datastore composed of a primary with fallbacks."""

    def __init__(self, primary, *fallbacks):
        self.primary = primary
        self.fallbacks = list(fallbacks)

    @property
    def _all_stores(self):
        return [self.primary] + self.fallbacks

    def contains(self, alias):
        """Checks if a dataframe with this alias is in the store."""
        return any((store.contains(alias) for store in self._all_stores))

    def ls(self, alias, glob=None, regex=".*", show_hidden=False):
        """Lists file paths for the given alias."""
        with self._store_for_alias(alias) as store:
            return store.ls(alias, glob, regex, show_hidden)

    def open(self, alias, logical_path, mode="r"):
        """Opens the given path as a file-like object."""
        if mode in WRITE_MODES:
            return self.primary.open(alias, logical_path, mode)

        with self._store_for_alias(alias) as store:
            return store.open(alias, logical_path, mode)

    def files(self, alias, glob=None, regex=".*", show_hidden=False):
        """Returns a dataframe of file paths"""
        with self._store_for_alias(alias) as store:
            return store.files(alias, glob, regex, show_hidden)

    def load_dataframe(self, alias):
        """Loads a dataframe from the store."""
        with self._store_for_alias(alias) as store:
            return store.load_dataframe(alias)
        raise Exception("Didn't find {0} in any stores.".format(alias))

    def store_dataframe(self, alias, dataframe):
        """Stores a dataframe in the cache."""
        self.primary.store_dataframe(alias, dataframe)

    @contextmanager
    def _store_for_alias(self, alias):
        for store in self._all_stores:
            if store.contains(alias):
                yield store
                return

        raise Exception("No store contains {0}".format(alias))


class InMemoryDatastore(object):
    # pylint: disable=no-self-use,unused-argument,invalid-name
    """Datastore which just holds some dataframes in memory. For use in tests."""

    def __init__(self):
        self.dataframe_by_alias = {}

    def contains(self, alias):
        """Checks if a dataframe with this alias is in the store."""
        return alias in self.dataframe_by_alias

    def ls(self, alias, glob=None, regex=".*", show_hidden=False):
        """Lists file paths for the given alias."""
        return NotImplementedError(
            "file transforms not implemented for in-memory datastore"
        )

    def open(self, alias, logical_path, mode="r"):
        """Opens the given path as a file-like object."""
        return NotImplementedError(
            "file transforms not implemented for in-memory datastore"
        )

    def files(self, alias, glob=None, regex=".*", show_hidden=False):
        """Returns a dataframe of file paths"""
        return NotImplementedError(
            "file transforms not implemented for in-memory datastore"
        )

    def load_dataframe(self, alias):
        """Loads a dataframe from the store."""
        return self.dataframe_by_alias[alias]

    def store_dataframe(self, alias, dataframe):
        """Stores a dataframe in the cache."""
        self.dataframe_by_alias[alias] = dataframe


class LocalDiskDatastore(object):
    # pylint: disable=invalid-name
    """Dataframe Cache which stores dataframes on-disk as parquet files."""

    def __init__(
        self, working_dir, path_prefix=None, infer_schema=True, file_format=None
    ):
        self.working_dir = working_dir
        self.path_prefix = path_prefix
        self.infer_schema = infer_schema
        self.file_format = file_format

    @property
    def _spark_session(self):
        return SparkSession.builder.getOrCreate()

    def contains(self, alias):
        """Checks if a dataframe with this alias is in the store."""
        return self._detect_dataset_format(alias) is not None

    def ls(self, alias, glob=None, regex=".*", show_hidden=False):
        """Lists file paths for the given alias."""
        if not self._detect_dataset_format(alias) == "dir":
            raise Exception("{0} is not a directory, can't list files.".format(alias))

        dataset_dir = self._get_path(alias)

        full_paths = [
            os.path.join(dirpath, name)
            for (dirpath, _, filenames) in os.walk(dataset_dir)
            for name in filenames
        ]

        logical_paths = [os.path.relpath(path, dataset_dir) for path in full_paths]

        globbed = [
            path for path in logical_paths if glob is None or PurePath(path).match(glob)
        ]
        regexed = [path for path in globbed if re.match(regex, path) is not None]
        return [
            _file_status(path, dataset_dir)
            for path in regexed
            if show_hidden or not (path.startswith("_") or path.startswith("."))
        ]

    def open(self, alias, logical_path, mode="r"):
        """Opens the given path as a file-like object."""
        physical_path = os.path.join(self._get_path(alias), logical_path)
        # Have to make sure all directories exist if we're writing.
        if mode in WRITE_MODES:
            try:
                os.makedirs(os.path.dirname(physical_path))
            except OSError as e:
                # Note: use FileExistsError once we drop support for python 2.
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
        return open(physical_path, mode)

    def files(self, alias, glob=None, regex=".*", show_hidden=False):
        """Returns a dataframe of file paths"""
        file_statuses = self.ls(alias, glob, regex, show_hidden)
        rows = [(fs.path, fs.size, fs.modified) for fs in file_statuses]
        return self._spark_session.createDataFrame(
            rows, schema="path: string, size: long, modified: long"
        )

    def load_dataframe(self, alias):
        """Loads a dataframe from the store."""
        file_format = self._detect_dataset_format(alias)
        if file_format == "csv" or file_format == "json":
            expected_file_format = (
                file_format if self.file_format is None else self.file_format
            )
            return self._load_file_dataframe(alias, file_format=expected_file_format)

        if file_format == "dir":
            return self._load_parquet_dataframe(alias)

        raise Exception("Unknown format {0}".format(file_format))

    def store_dataframe(self, alias, dataframe):
        """Stores a dataframe in the cache."""
        path = self._get_path(alias)
        dataframe.write.parquet(path)

    def _load_file_dataframe(self, alias, file_format="csv"):
        base_path = self._get_path(alias)
        file_path = base_path + ".schema.json"
        log.info("Looking for schema at %s", file_path)
        schema = None
        if os.path.isfile(file_path):
            log.info("Loading schema from %s", file_path)
            with open(file_path) as schema_file:
                schema = _foundry_schema_to_spark_schema(json.load(schema_file))

        return _load_table(
            self._spark_session,
            base_path + "." + file_format,
            file_format=file_format,
            schema=schema,
            infer_schema="true" if self.infer_schema else "false",
        )

    def _load_parquet_dataframe(self, alias):
        return self._spark_session.read.parquet(self._get_path(alias))

    def _detect_dataset_format(self, alias):
        base_path = self._get_path(alias)
        if os.path.exists(base_path + ".csv"):
            return "csv"
        if os.path.exists(base_path + ".json"):
            return "json"
        if os.path.exists(base_path) and os.path.isdir(base_path):
            return "dir"

        return None

    def _get_path(self, alias):
        if self.path_prefix is not None:
            alias = alias.replace(self.path_prefix, "", 1)
        return os.path.join(self.working_dir, alias.lstrip("/"))


def _file_status(path, dataset_dir):
    full_path = os.path.join(dataset_dir, path)
    stat = os.stat(full_path)
    return FileStatus(path, stat.st_size, int(stat.st_mtime * 1000))


def _load_table(
    spark_session, file_path, infer_schema="true", schema=None, file_format="csv"
):
    """Loads a dataset from a file on disk."""
    log.info("Loading data from %s", file_path)

    reader = None
    if file_format == "csv":
        reader = (
            spark_session.read.format(file_format)
            .option("escape", '"')
            .option("header", "true")
            .option("mode", "FAILFAST")
            .option("multiLine", "true")
        )
    elif file_format == "json":
        reader = spark_session.read.format(file_format).option("mode", "FAILFAST")

    if reader is None:
        raise ValueError("Unsupported file_format", file_format)

    if schema:
        reader.schema(schema)
    else:
        reader.option("inferSchema", infer_schema)

    return reader.load(file_path)


# pylint: disable=too-many-return-statements, too-many-branches
def _foundry_field_to_spark_type(foundry_field):
    """Converts a foundry FieldSchema into a spark data type."""
    foundry_type = foundry_field["type"]
    spark_simple_type = _foundry_type_to_spark_type(foundry_field, foundry_type)
    if spark_simple_type is not None:
        return spark_simple_type
    elif foundry_type == "ARRAY":
        array_subfield = foundry_field["arraySubtype"]
        array_subtype = foundry_field["arraySubtype"]["type"]

        if array_subtype in ["ARRAY", "MAP", "STRUCT"]:
            raise NotImplementedError(
                f"Complex data type {0} not yet supported as subtype of ARRAY{foundry_type}"
            )

        spark_array_subtype = _foundry_type_to_spark_type(array_subfield, array_subtype)
        if spark_array_subtype is None:
            raise ValueError("Unknown foundry array type: {0}".format(array_subtype))

        return T.ArrayType(spark_array_subtype)
    elif foundry_type in ["MAP", "STRUCT"]:
        raise NotImplementedError(
            "Complex data type {0} not yet supported".format(foundry_type)
        )
    else:
        raise ValueError("Unknown foundry data type: {0}".format(foundry_type))


def _foundry_type_to_spark_type(foundry_field, foundry_type):
    if foundry_type == "BINARY":
        return T.BinaryType()
    elif foundry_type == "BOOLEAN":
        return T.BooleanType()
    elif foundry_type == "BYTE":
        return T.ByteType()
    elif foundry_type == "DATE":
        return T.DateType()
    elif foundry_type == "DOUBLE":
        return T.DoubleType()
    elif foundry_type == "FLOAT":
        return T.FloatType()
    elif foundry_type == "INTEGER":
        return T.IntegerType()
    elif foundry_type == "LONG":
        return T.LongType()
    elif foundry_type == "SHORT":
        return T.ShortType()
    elif foundry_type == "STRING":
        return T.StringType()
    elif foundry_type == "TIMESTAMP":
        return T.TimestampType()
    elif foundry_type == "DECIMAL":
        precision = foundry_field["precision"]
        scale = foundry_field["scale"]
        return T.DecimalType(precision, scale)
    else:
        return None


def _foundry_schema_to_spark_schema(foundry_schema):
    """Converts a foundry Schema into a spark StructType."""
    foundry_fields = foundry_schema["schema"]["fieldSchemaList"]
    spark_fields = [_foundry_field_to_spark_field(field) for field in foundry_fields]
    return T.StructType(spark_fields)


def _foundry_field_to_spark_field(foundry_field):
    """Converts a foundry FieldSchema into a spark StructField."""
    spark_type = _foundry_field_to_spark_type(foundry_field)
    return T.StructField(foundry_field["name"], spark_type)
