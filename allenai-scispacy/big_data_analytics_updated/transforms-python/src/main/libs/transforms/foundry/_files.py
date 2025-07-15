# Copyright 2017 Palantir Technologies, Inc.
"""API for raw file access in Foundry datasets."""
import abc
import collections
import heapq
import io
import logging
import os
import re
from datetime import datetime

from dateutil import parser, tz
from future import utils
from pyspark.sql import types

from transforms._errors import ConjureError, ErrorCode, ErrorType

from .. import _utils
from . import _foundryfs, _glob

log = logging.getLogger(__name__)


FileStatus = collections.namedtuple("FileStatus", ["path", "size", "modified"])


class UnknownPackingHeuristicError(ConjureError, ValueError):
    """Raised when the packing_heuristic argument to the files() method is not a recognized heuristic."""

    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:UnknownPackingHeuristic"
    )


class _BaseFileSystem(utils.with_metaclass(abc.ABCMeta)):
    """Interface for a filesystem to implement."""

    @property
    @abc.abstractmethod
    def _foundry(self):
        """Return the Foundry instance."""

    @abc.abstractmethod
    def open(self, path, mode="r", **kwargs):
        """Open a file for reading or writing."""

    @abc.abstractmethod
    def ls(self, glob=None, regex=".*", show_hidden=False):
        """Yields a listing of files within the filesystem."""

    @_utils.driver_only
    def files(self, glob=None, regex=".*", show_hidden=False, packing_heuristic=None):
        """Create a :class:`~pyspark.sql.DataFrame` containing the paths accessible within this dataset.

        The :class:`~pyspark.sql.DataFrame` is partitioned by file size where each partition contains file paths
        whose combined size is at most ``spark.files.maxPartitionBytes`` bytes or a single file if that file is
        itself larger than ``spark.files.maxPartitionBytes``. The size of a file is calculated as its on-disk
        file size plus the ``spark.files.openCostInBytes``.

        Args:
            glob (str, optional): A unix file matching pattern. Also supports globstar.
            regex (str, optional): A regex pattern against which to match filenames.
            show_hidden (bool, optional): Include hidden files, those prefixed with '.' or '_'.
            packing_heuristic (str, optional): Specify a heuristic to use for bin-packing files into spark partitions.
                                               Possible choices are:
                                                 - "ffd" (First Fit Decreasing)
                                                 - "wfd" (Worst Fit Decreasing)
                                               WFD tends to result in a slightly worse packing, but is much faster,
                                               so is recommended for datasets containing very large numbers of files.
                                               If a heuristic is not specified, one will be selected automatically.

        Returns:
            pyspark.sql.DataFrame of (path, size, modified)
        """
        ss = self._fsm.spark_session

        file_partitions = _partition_files_by_size(
            self.ls(glob=glob, regex=regex, show_hidden=show_hidden),
            # PySpark doesn't expose default values for configs not explicitly set, so we use the Java methods
            ss._jsparkSession.sessionState().conf().filesMaxPartitionBytes(),
            ss._jsparkSession.sessionState().conf().filesOpenCostInBytes(),
            heuristic=packing_heuristic,
        )

        files_rdd = ss.sparkContext.parallelize(
            file_partitions, len(file_partitions)
        ).flatMap(lambda x: x, preservesPartitioning=True)

        return ss.createDataFrame(
            files_rdd,
            schema=types.StructType(
                [
                    types.StructField("path", types.StringType()),
                    types.StructField("size", types.LongType()),
                    types.StructField("modified", types.LongType()),
                ]
            ),
        )

    @property
    @_utils.driver_only
    def _fsm(self):
        return self._foundry._foundry_spark_manager


class EmptyFileSystem(_BaseFileSystem):
    """File system object that is read-only and completely empty."""

    # pylint: disable=super-init-not-called

    def __init__(self, foundry):
        self.__foundry = foundry

    @property
    def _foundry(self):
        return self.__foundry

    def open(self, path, mode="r", **kwargs):
        raise IOError(f"File not found: {path}")

    def ls(self, glob=None, regex=".*", show_hidden=False):
        # We iter so Python treats us as a generator
        return iter([])

    @property
    def _root_path(self):
        return None


class FoundryFileSystem(_BaseFileSystem):
    """Standard file system object for reading directly from FoundryFS."""

    # pylint: disable=super-init-not-called

    def __init__(self, foundry, rid, end_ref, start_txrid=None):
        # TODO(gatesn): #215 - Support start transaction rid
        self.__foundry = foundry
        self._rid = rid
        self._end_ref = end_ref
        self._start_txrid = start_txrid

    @property
    def _foundry(self):
        return self.__foundry

    @property
    def _foundry_fs(self):
        return self._foundry._foundry_fs

    def open(self, path, mode="r", **kwargs):
        if not mode.startswith("w"):
            return self._foundry_fs.open(
                self._foundry_fs.make_qualified(self._root_path + "/" + path),
                mode=mode,
                **kwargs,
            )

        # The create call may fail if our end_ref isn't an open transaction. To
        # prevent unnecessary catalog load, we only perform this check if the create
        # call *does* fail. Let's diagnose errors not prognose them!
        try:
            return self._foundry_fs.create(
                self._foundry_fs.make_qualified(self._root_path + "/" + path),
                mode=mode,
                **kwargs,
            )
        except Exception as e:
            txn = next(
                self._foundry._service_provider.catalog().get_reverse_transactions(
                    self._foundry.auth_header,
                    self._rid,
                    self._end_ref,
                    True,
                    page_size=1,
                ),
                {},
            ).get("transaction")

            if not txn or txn["status"] != "OPEN":
                raise ValueError(
                    "Opening a file for writing requires end_ref be an open transaction. "
                    f"Instead from end_ref {self._end_ref} got transaction {txn}"
                ) from e

            # Otherwise raise whatever it was
            raise

    def ls(self, glob=None, regex=".*", show_hidden=False):
        """List the contents of the root directory in the view.

        Args:
            glob (str, optional): A unix file matching pattern. Also supports globstar.
            regex (str, optional): A regex pattern against which to match filenames.
            show_hidden (bool, optional): Include hidden files, those prefixed with '.' or '_'.

        Yields:
            FileStatus: The HDFS path, file size (bytes), modified timestamp (ms since January 1, 1970 UTC)
        """
        if glob:
            matcher = re.compile(_glob.glob2regex(glob))
        else:
            matcher = re.compile(regex)

        for file_resource in self._list_files(show_hidden):
            if not matcher.match(get_path(file_resource)):
                continue

            yield file_status(file_resource)

    def _list_files(self, show_hidden):
        return files_in_view(
            self._foundry,
            self._rid,
            self._end_ref,
            start_txn=self._start_txrid,
            show_hidden=show_hidden,
        )

    @property
    def _root_path(self):
        """str: The root HDFS path for the view."""
        view_path = _foundryfs.FoundryFilePaths().get_dataset_view_path(
            self._rid, self._end_ref, start_txrid=self._start_txrid
        )
        return self._foundry_fs.make_qualified(view_path)


class _ROFoundryFileSystem(FoundryFileSystem):
    """Read-only file system that swallows file writes."""

    def open(self, path, mode="r", **kwargs):
        """Open a FoundryFS file for writing.

        Args:
            path (str): The logical path of the file in the view.
            *kwargs: Remaining keyword args passed to ``io.open``.

        Returns:
            File: a Python file-like object attached to the stream.
        """
        if not mode.startswith("w"):
            # pylint: disable=consider-using-with
            return super().open(path, mode=mode, **kwargs)
        # pylint: disable=consider-using-with, unspecified-encoding
        return io.open(os.devnull, mode, **kwargs)


def is_hidden(logical_path):
    """bool: whether the logical path is considered a hidden file."""
    base = os.path.basename(logical_path)
    return base.startswith(".") or base.startswith("_")


def _partition_files_wfd(file_statuses, max_partition_size, open_cost):
    """Pack an iterable of FileStatus namedtuples into partitions of the maximum given size.
    Assigns files to partitions using "Worst Fit Decreasing" (WFD) implemented with a priority queue.
    """
    # Sort by file size decreasing
    # Packing the largest files first ensures we end up with more evenly distributed buckets.
    file_statuses.sort(key=lambda fs: fs.size, reverse=True)

    log.info(
        "Partitioning %s files with max size %s and open cost %s using WFD heuristic",
        len(file_statuses),
        max_partition_size,
        open_cost,
    )

    bins = [(0, 0, [])]
    bin_ix = 1  # Used as a tie-breaker when sorting so the actual bin items are not considered.
    for fs in file_statuses:
        size, ix, items = bins[0]
        cost = fs.size + open_cost
        if size == 0 or size + cost <= max_partition_size:
            items.append(fs)
            heapq.heapreplace(bins, (size + cost, ix, items))
        else:
            heapq.heappush(bins, (fs.size, bin_ix, [fs]))
            bin_ix += 1

    return [files for _, _, files in bins]


def _partition_files_ffd(file_statuses, max_partition_size, open_cost):
    """Pack an iterable of FileStatus namedtuples into partitions of the maximum given size.
    Assigns files to partitions using "First Fit Decreasing" (FFD).
    """
    # Sort by file size decreasing
    # Packing the largest files first ensures we end up with more evenly distributed buckets.
    file_statuses.sort(key=lambda fs: fs.size, reverse=True)

    log.info(
        "Partitioning %s files with max size %s and open cost %s using FFD heuristic",
        len(file_statuses),
        max_partition_size,
        open_cost,
    )

    bins = [([], max_partition_size)]
    ix = 0
    largest_skipped_space = None
    for fs in file_statuses:
        if largest_skipped_space is not None and fs.size <= largest_skipped_space:
            # File could fit in an earlier bin, reset searching from the start.
            ix = 0
            largest_skipped_space = None

        file_cost = fs.size + open_cost

        packed = False
        while ix < len(bins):
            b, cap = bins[ix]
            if file_cost <= cap or cap == max_partition_size:
                # File fits in this bin (or bin is empty) so pack it.
                b.append(fs)
                bins[ix] = (b, cap - file_cost)
                packed = True
                break

            if largest_skipped_space is None or cap > largest_skipped_space:
                largest_skipped_space = cap

            ix += 1

        if not packed:
            # Didn't fit in any existing bin, create a new one.
            bins.append(([fs], max_partition_size - fs.size))

    return [b for b, _ in bins]


FIRST_FIT_DECREASING = "ffd"
WORST_FIT_DECREASING = "wfd"

PACKING_HEURISTICS = {
    FIRST_FIT_DECREASING: _partition_files_ffd,
    WORST_FIT_DECREASING: _partition_files_wfd,
}


def _partition_files_by_size(file_statuses, max_partition_size, open_cost, heuristic):
    # Force any generators into a list.
    file_statuses = list(file_statuses)

    if heuristic is None:
        # If unspecified, use a heuristic to choose the heuristic!
        heuristic = (
            WORST_FIT_DECREASING
            if len(file_statuses) >= 100000
            else FIRST_FIT_DECREASING
        )
        log.info(
            "Selecting packing heuristic '%s' based on file count: %d",
            heuristic,
            len(file_statuses),
        )

    normalized_heuristic_name = heuristic.lower()
    if normalized_heuristic_name not in PACKING_HEURISTICS:
        heuristic_names = list(PACKING_HEURISTICS.keys())
        raise UnknownPackingHeuristicError(
            f"Invalid value for 'heuristic'.  Valid algorithms are: {heuristic_names}"
        )

    partition_func = PACKING_HEURISTICS[normalized_heuristic_name]
    return partition_func(file_statuses, max_partition_size, open_cost)


def files_in_view(foundry, rid, end_ref, start_txn=None, show_hidden=False):
    """Return a stream of non-hidden files in the given catalog view."""
    catalog = foundry._service_provider.catalog()
    return (
        fr
        for fr in catalog.get_dataset_view_files(
            foundry.auth_header,
            rid,
            end_ref,
            start_txrid=start_txn,
            include_open=True,
            page_size=10000,
        )
        if show_hidden or not is_hidden(get_path(fr))
    )


def file_status(file_resource):
    return FileStatus(
        path=get_path(file_resource),
        size=get_size(file_resource),
        modified=get_modified(file_resource),
    )


def get_path(file_resource):
    return file_resource["logicalPath"]


def get_modified(file_resource):
    string_time = file_resource["timeModified"]
    try:
        dt = parser.isoparse(string_time)
    except ValueError:
        log.warning("Could not parse date %s using ISO format", string_time)
        dt = parser.parse(string_time)
    utc_epoch = datetime(1970, 1, 1, tzinfo=tz.tzutc())
    # in milliseconds
    utc_timestamp = (dt - utc_epoch).total_seconds() * 1000
    return int(utc_timestamp)


def get_size(file_resource):
    return (file_resource.get("fileMetadata") or {}).get("length")
