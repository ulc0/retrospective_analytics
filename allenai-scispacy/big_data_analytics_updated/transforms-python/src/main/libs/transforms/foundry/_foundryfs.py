# Copyright 2017 Palantir Technologies, Inc.
import contextlib
import io
import json
import logging
import os
import shutil
import tempfile
import threading
from typing import Any, Optional

import pyspark
from py4j import java_gateway

from transforms import _java_utils as j

log = logging.getLogger(__name__)


class FoundryFS(j.JavaProxy):
    """A pickleable proxy object wrapping Foundry FS."""

    def __init__(
        self,
        auth_header,
        service_provider,
        temp_creds_token,
        spark_session,
        clone_hadoop_conf,
    ) -> None:
        self._jauth_header = j.to_auth_header(auth_header)
        self._jtemp_creds_token = j.to_temp_creds_token(temp_creds_token)
        self._jcatalog_uris = j.to_set(
            service_provider.services_config["catalog"]["uris"]
        )
        self._jssl_conf = j.to_optional(
            j.object_mapper().readValue(
                json.dumps(service_provider.service_discovery_config["security"]),
                java_gateway.get_java_class(
                    self._jvm.com.palantir.conjure.java.api.config.ssl.SslConfiguration
                ),
            )
        )

        # The correct behaviour here is to clone the Hadoop configuration to prevent multiple instantiations of
        # FoundryFS modifying the same underlying Hadoop configuration. This is currently configurable to avoid
        # breaking changes but will be cloned by default in the future.
        if clone_hadoop_conf:
            self._jhadoop_conf = (
                spark_session._jsparkSession.sessionState().newHadoopConf()
            )
        else:
            self._jhadoop_conf = (
                pyspark.SparkContext.getOrCreate()._jsc.hadoopConfiguration()
            )

        super().__init__(self._create_fs())

    def create(self, path, mode="w", **kwargs):
        """Open the FoundryFS file for writing at the given path.

        The returned file object is not seekable and can only be written to.

        Args:
            path (str): The FoundryFS path to open.
            **kwargs: Extra kwargs to pass into the ``io.open`` call.

        Returns:
            file: A Python file-like object that can only be written to.
        """
        if not mode.startswith("w"):
            raise ValueError("Mode must start with 'w' for writing a Foundry file")

        def fifo_to_java(jos, fifo_path):
            jis = self._jvm.java.io.FileInputStream(fifo_path)
            bytes_copied = self._jvm.org.apache.commons.io.IOUtils.copyLarge(jis, jos)
            log.info("Copied %s bytes", bytes_copied)
            jos.close()

        return _java_pipe(
            self._proxy.create(j.to_hadoop_path(path)),
            fifo_to_java,
            mode=mode,
            **kwargs,
        )

    def list_files(self, path, recursive=False):
        """List the files underneath the given path.

        Args:
            path (str): The FoundryFS path to list files from.
            recursive (bool, optional): List files recursively.

        Yields:
            tuple of str, int, int: The HDFS path, file size, modified timestamp (ms since January 1, 1970 UTC)
        """
        jfile_iter = self._proxy.listFiles(j.to_hadoop_path(path), recursive)
        while jfile_iter.hasNext():
            jfile_status = jfile_iter.next()
            path = jfile_status.getPath().toString()
            size = jfile_status.getLen()
            modified = jfile_status.getModificationTime()
            yield path, size, modified

    def open(self, path: str, mode: str = "r", **kwargs: Any):
        """Open the FoundryFS file for reading at the given path.

        The returned file object is not seekable and can only be read from.

        Args:
            path (str): The FoundryFS path to open.
            **kwargs: Extra kwargs to pass into the ``io.open`` call.

        Returns:
            file: A Python file-like object that can only be read from.
        """
        if not mode.startswith("r"):
            raise ValueError("Mode must start with 'r' for reading a Foundry file")

        def java_to_fifo(jis, fifo_path):
            jos = self._jvm.java.io.FileOutputStream(fifo_path)
            bytes_copied = self._jvm.org.apache.commons.io.IOUtils.copyLarge(jis, jos)
            log.info("Copied %s bytes", bytes_copied)
            jos.close()

        return _java_pipe(
            self._proxy.open(j.to_hadoop_path(path)), java_to_fifo, mode=mode, **kwargs
        )

    def make_qualified(self, path):
        """Return a qualified path.

        Args:
            path (str): The path to qualify against the file system.

        Returns:
            str: The qualified path.
        """
        return self._proxy.makeQualified(j.to_hadoop_path(path)).toString()

    def _create_fs(self):
        return self._jvm.com.palantir.foundry.fs.FoundryFileSystemFactory.createFoundryFileSystemProvider(
            self._jcatalog_uris,
            j.to_optional(self._jssl_conf.get().trustStorePath()),
            j.to_optional(self._jssl_conf.get().trustStoreType().toString()),
            self._jhadoop_conf,
        ).apply(
            self._jauth_header.getBearerToken(), self._jtemp_creds_token
        )

    def _serialize_configuration(self, jconfiguration):
        return self._jvm.org.apache.commons.lang3.SerializationUtils.serialize(
            jconfiguration
        )

    def _deserialize_configuration(self, serialized):
        return self._jvm.org.apache.commons.lang3.SerializationUtils.deserialize(
            serialized
        )

    def __getstate__(self):
        """Return the values to be pickled."""
        temp_creds_token = (
            self._jtemp_creds_token.get().toString()
            if self._jtemp_creds_token.isPresent()
            else None
        )
        catalog_uris = j.object_mapper().writeValueAsString(self._jcatalog_uris)
        ssl_conf = j.object_mapper().writeValueAsString(self._jssl_conf.get())
        hadoop_conf = self._serialize_configuration(
            self._jvm.org.apache.spark.util.SerializableConfiguration(
                self._jhadoop_conf
            )
        )
        return {
            "auth_header": self._jauth_header.toString(),
            "temp_creds_token": temp_creds_token,
            "catalog_uris": catalog_uris,
            "ssl_conf": ssl_conf,
            "hadoop_conf": hadoop_conf,
        }

    def __setstate__(self, state):
        """Restore the pickleable values."""
        self._jauth_header = j.to_auth_header(state["auth_header"])
        self._jtemp_creds_token = j.to_temp_creds_token(state["temp_creds_token"])
        self._jcatalog_uris = j.object_mapper().readValue(
            state["catalog_uris"], java_gateway.get_java_class(self._jvm.java.util.Set)
        )
        self._jssl_conf = j.to_optional(
            j.object_mapper().readValue(
                state["ssl_conf"],
                java_gateway.get_java_class(
                    self._jvm.com.palantir.conjure.java.api.config.ssl.SslConfiguration
                ),
            )
        )
        self._jhadoop_conf = self._deserialize_configuration(
            state["hadoop_conf"]
        ).value()
        self._proxy = self._create_fs()


class FoundryFilePaths(j.JavaProxy):
    """Wrapper around com.palantir.foundry.fs.FoundryFilePaths."""

    def __init__(self) -> None:
        super().__init__(self._jvm.com.palantir.foundry.fs.FoundryFilePaths)

    def get_dataset_view_path(
        self, rid: str, end_ref: str, start_txrid: Optional[str] = None
    ) -> str:
        """Return the dataset view path for the given dataset and transaction range.

        Args:
            rid (str): The dataset resource identifier.
            end_ref (str): The end branch or transaction of the view.
            start_txrid (str, optional): Optionally a start transaction rid.

        Returns:
            str: The dataset view path, needs to be qualified before use.
        """
        return self._proxy.getDatasetViewPath(
            j.to_rid(rid),
            j.to_optional(start_txrid, j.to_rid),
            j.to_transaction_ref(end_ref),
        ).toString()


@contextlib.contextmanager
def _get_tmpdir():
    """Self-deleting temporary directory."""
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


@contextlib.contextmanager
def _safe_closing(thing):
    try:
        yield thing
    finally:
        try:
            thing.close()
        except:  # pylint: disable=bare-except
            log.exception("Failed to close %s", thing)


@contextlib.contextmanager
def _java_pipe(jstream, pipe_func, **open_kwargs):
    """Create a thread to run `pipe_func`."""
    with _get_tmpdir() as tmpdir, _safe_closing(jstream) as js:
        fifo_path = os.path.join(tmpdir, "fifo")
        os.mkfifo(fifo_path)

        thread = threading.Thread(target=pipe_func, args=(js, fifo_path))
        thread.start()

        # pylint: disable=consider-using-with, unspecified-encoding
        pipe = io.open(fifo_path, **open_kwargs)
        try:
            yield pipe
        finally:
            pipe.close()
            thread.join()
