from os import walk as _walk, sep as _sep
from os.path import join as _join, normpath as _normpath
from re import compile as _compile
from typing import (
    Any as _Any,
    BinaryIO as _BinaryIO,
    Iterable as _Iterable,
    Optional as _Optional,
    TextIO as _TextIO,
    Tuple as _Tuple,
    Union as _Union,
)

from pyspark.sql import DataFrame as _DataFrame, SparkSession as _SparkSession
from pyspark.sql.types import (
    LongType as _LongType,
    StringType as _StringType,
    StructField as _StructField,
    StructType as _StructType,
)

# TODO: re-add glob2regex mock once it is needed. The code is commented in this file.
#  At the time of writing this I couldn't get tests to work with it within gradle, so it's commented out for now.
# from transforms.foundry._glob import glob2regex as _glob2regex


class MockFactory:
    def __init__(self, session, root_path):
        SparkSessionHolder.session = session
        self._root_path = root_path

    def transform_input(self: "MockFactory", path: str) -> "MockTransformInput":
        return MockTransformInput(_join(self._root_path, path))


class SparkSessionHolder:
    session = None


class MockTransformInput:
    def __init__(self: "MockTransformInput", path: str) -> None:
        self._path = path
        self._filesystem = MockFileSystem(path)

    def filesystem(self: "MockTransformInput") -> "MockFileSystem":
        return self._filesystem


class MockFileSystem:
    def __init__(self: "MockFileSystem", path: str) -> None:
        self._path = path

    def files(
        self: "MockFileSystem",
        glob: str = None,
        regex: str = ".*",
        show_hidden: bool = False,
    ) -> _DataFrame:
        return SparkSessionHolder.session.createDataFrame(
            self.ls(glob=glob, regex=regex, show_hidden=show_hidden),
            _StructType(
                [
                    _StructField("path", _StringType()),
                    _StructField("size", _LongType()),
                    _StructField("modified", _LongType()),
                ]
            ),
        )

    def ls(
        self: "MockFileSystem",
        glob: str = None,
        regex: str = ".*",
        show_hidden: bool = False,
    ) -> _Iterable[_Tuple[str, int, int]]:
        # See TODO at head of file.
        # if glob:
        #    matcher = _compile(_glob2regex(glob))
        # else:
        #    matcher = _compile(regex)
        matcher = _compile(regex)

        for root, _, files in _walk(self._path):
            for name in files:
                full_path = _join(root, name)
                if not show_hidden and _is_hidden(full_path):
                    continue
                if not matcher.match(full_path):
                    continue
                yield _file_status(full_path)

    def open(
        self: "MockFileSystem", path: str, mode: str = "r", **kwargs: _Any
    ) -> _Union[_BinaryIO, _TextIO]:
        return open(path, mode, **kwargs)


def _is_hidden(full_path: str) -> bool:
    pieces = _normpath(full_path).split(_sep)
    for piece in pieces:
        if piece.startswith("_") or piece.startswith("."):
            return True
    return False


def _file_status(full_path: str) -> _Tuple[str, int, int]:
    return (full_path, 0, 0)
