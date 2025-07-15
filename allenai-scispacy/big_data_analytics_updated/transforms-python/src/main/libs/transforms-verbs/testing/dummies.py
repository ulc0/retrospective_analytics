#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.

from pyspark.sql import SparkSession
from transforms.api import TransformInput, TransformOutput


# pylint: disable=too-few-public-methods
class DummyTransformContext(object):
    """Dummy TransformContext object used to pass to transforms which need access to the spark_session."""

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.auth_header = "Bearer foobar1234"
        self.fallback_branches = []
        self.parameters = {}


class DummyDfReader:
    def clear_cache(self):
        pass


class DummyDfWriter:
    def set_mode(self, _mode):
        pass


class DummyFsBuilder:
    def set_mode(self, _mode):
        pass


class DummyTransformOutput(TransformOutput):
    """Dummy TransformOutput object."""

    def __init__(self, datastore, alias, rid="ri.dataset.foo.0001"):
        super().__init__(
            rid=rid,
            branch="master",
            txrid="dummy-txrid",
            dfreader=DummyDfReader(),
            dfwriter=DummyDfWriter(),
            fsbuilder=DummyFsBuilder(),
        )
        self.datastore = datastore
        self.alias = alias

    def write_dataframe(self, dataframe, **kwargs):  # pylint: disable=arguments-differ
        """Write out a dataframe to the backing store."""
        self.datastore.store_dataframe(self.alias, dataframe)

    def filesystem(self):
        """Returns the backing filesystem for this input."""
        return DummyFileSystem(self.datastore, self.alias)


# pylint: disable=super-init-not-called
class DummyTransformInput(TransformInput):
    """Dummy TransformInput object."""

    def __init__(self, datastore, alias):
        self.datastore = datastore
        self.alias = alias

    def dataframe(self):
        """Read this input as a dataframe."""
        return self.datastore.load_dataframe(self.alias)

    def filesystem(self):
        """Returns the backing read-only filesystem for this input."""
        return DummyFileSystem(self.datastore, self.alias)


class DummyFileSystem(object):
    # pylint: disable=unused-argument,invalid-name
    """Dummy Filesystem object."""

    def __init__(self, datastore, alias):
        self.datastore = datastore
        self.alias = alias

    def open(self, path, mode="r", **kwargs):
        """Open file"""
        # Ignore mode and kwargs for now.  They are not often used.
        return self.datastore.open(self.alias, path, mode)

    def ls(self, glob=None, regex=".*", show_hidden=False):
        """List files"""
        return self.datastore.ls(self.alias, glob, regex, show_hidden)

    def files(self, glob=None, regex=".*", show_hidden=False):
        """Returns a dataframe of file paths."""
        return self.datastore.files(self.alias, glob, regex, show_hidden)
