#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""
Classes to access the model state
"""

import abc
import os
import random
import shutil
import zipfile
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Iterator, Optional


class AbstractModelStateReader(abc.ABC):
    """
    Base interface for model state readers. Future ModelStateReaders should implement this interface,
    not the ModelStateReader class which assumes the model state is extracted to a temporary directory.
    """

    @abc.abstractmethod
    def extract_to_temp_dir(self, root_dir: Optional[str] = None) -> "AbstractContextManager[str]":
        """
        Retrieves the model version's state and returns a temporary directory containing its contents.
        This method is to be used in a context manager, and will perform cleanup of the returned temporary
        directory.

        Not all ModelStateReaders implement this method, but the model state reader used for transforms
        must implement it for back-compat reasons. Users are encourage to use the open/dir/mkdir methods
        instead as this allows for more efficient re-use of the weights when they're already extracted.

        :param root_dir: If specified, a locally-accessible
            directory to use as the root of the temporary
            directory
        """

    @abc.abstractmethod
    def extract(self, destination_path: str) -> None:
        """
        Extracts the model version's state to the destination path provided by the user. No cleanup of the
        user-specified path will be performed.
        """

    @abc.abstractmethod
    def open(self, asset_file_path: str, mode: str = "rb") -> IO:
        """
        Open a file-like object in the backing store for reading.
        This method is to be used in a context manager.
        """

    @abc.abstractmethod
    def dir(self, asset_file_dir: str) -> str:
        """
        Returns the path to a directory in the backing store for reading from.
        Throws if the provided path is not a directory.
        This method does not need a context manager.
        """

    def cleanup(self):
        """
        Optional cleanup method, can be overriden by subclasses to remove temporary resources.
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class ModelStateReader(AbstractModelStateReader, abc.ABC):
    """
    Retrieves a model version's state and extracts it to a temporary directory.
    The "extract" method is intended to be overridden by subclasses.

    This was originally the base class instead of AbstractModelStateReader, but the interface
    was extracted out. The classname here should remain the same to avoid breaking other libraries
    depending on this implementation. Subclasses should prefer the AbstractModelStateReader base class.
    """

    def __init__(self, tmp_dir: Optional[TemporaryDirectory] = None):
        self.__temp_dir = tmp_dir

    @contextmanager
    def extract_to_temp_dir(self, root_dir: Optional[str] = None) -> Iterator[str]:
        tmp_dir = TemporaryDirectory(dir=root_dir)
        self.extract(tmp_dir.name)
        try:
            yield tmp_dir.name
        finally:
            tmp_dir.cleanup()

    def open(self, asset_file_path: str, mode: str = "rb") -> IO:
        if self.__temp_dir is None:
            self.__temp_dir = TemporaryDirectory()
            self.extract(self.__temp_dir.name)

        return open(os.path.join(self.__temp_dir.name, asset_file_path), mode)

    def dir(self, asset_file_dir: str) -> str:
        if self.__temp_dir is None:
            self.__temp_dir = TemporaryDirectory()
            self.extract(self.__temp_dir.name)

        path = os.path.join(self.__temp_dir.name, asset_file_dir)
        assert os.path.isdir(path), f"'{asset_file_dir}' is not a directory (full path: {path})"
        return path

    def cleanup(self):
        if self.__temp_dir is not None:
            self.__temp_dir.cleanup()
            self.__temp_dir = None


class LocalDirectoryModelStateReader(AbstractModelStateReader):
    """
    Uses a directory already on the filesystem to provide model contents,
    adding symlinks to the directory instead of copying/unzipping.
    """

    def __init__(self, model_path: str):
        super().__init__()
        self.model_path = Path(model_path).resolve()

    @contextmanager
    def extract_to_temp_dir(self, root_dir: Optional[str] = None) -> Iterator[str]:
        if not root_dir:
            yield self.model_path.as_posix()
        else:
            temp_symlink = Path(root_dir) / f"__model_{random.randint(1000, 9999)}"
            try:
                temp_symlink.symlink_to(self.model_path, target_is_directory=True)
                yield temp_symlink.as_posix()
            finally:
                temp_symlink.unlink()

    def extract(self, destination_path: str) -> None:
        """
        'Extracts' the directory by creating a new symlink instead of copying data.
        """
        resolved_destination_path = Path(destination_path).resolve()
        for file in self.model_path.iterdir():
            dest = resolved_destination_path / file.name
            dest.symlink_to(file, target_is_directory=file.is_dir())

    def open(self, asset_file_path: str, mode: str = "rb") -> IO:
        return open(os.path.join(self.model_path, asset_file_path), mode)

    def dir(self, asset_file_dir: str) -> str:
        path = os.path.join(self.model_path, asset_file_dir)
        assert os.path.isdir(path), f"'{asset_file_dir}' is not a directory (full path: {path})"
        return path


class ZipModelStateReader(ModelStateReader):
    """
    Uses a model.zip available on the filesystem to provide model contents.
    """

    def __init__(self, file: IO[bytes]):
        super().__init__()
        self.__file = file

    def extract(self, destination_path: str) -> None:
        """
        Extracts the contents of model.zip to the destination path provided by the user.
        No cleanup of the user-specified path will be performed.
        """
        with zipfile.ZipFile(self.__file, "r") as zip_ref:
            zip_ref.extractall(destination_path)


class SubdirectoryModelStateReader(AbstractModelStateReader):
    """
    Reads files from a subdirectory of the model state. This is useful for
    custom serializers, as it ensures that they read from the correct directory
    per argument. The reader does not protect against reading from outside the
    subdirectory (e.g. by using `..` in the path).

    Example use:
    ```
    reader = SubdirectoryModelStateReader("dir", backing_reader)
    with writer.open("model.json") as f:
        model = json.read(f)
    ```
    This will attempt to read from a file at `dir/model.json` in the backing store.
    """

    def __init__(self, subdir: str, reader: AbstractModelStateReader):
        super().__init__()
        self.subdir = subdir
        self.reader = reader

    def extract_to_temp_dir(self, _root_dir=None):
        raise Exception("Extract to temp dir not supported for custom serializers.")

    def open(self, asset_file_path: str, mode: str = "rb") -> IO:
        return self.reader.open(os.path.join(self.subdir, asset_file_path), mode)

    def dir(self, asset_file_dir: str):
        return self.reader.dir(os.path.join(self.subdir, asset_file_dir))

    def extract(self, destination_path: str) -> None:
        self.reader.extract(destination_path)


class ModelStateWriter(abc.ABC):
    """
    Writes a model version's state
    to a backing resource
    """

    @abc.abstractmethod
    def open(self, asset_file_path: str, mode: str = "wb") -> "AbstractContextManager[IO]":
        """
        Open a file-like object in the backing store for writing.
        """

    @abc.abstractmethod
    def mkdir(self, asset_dir_path: str) -> str:
        """
        Create a directory in the backing store and return a path to it.
        """

    @abc.abstractmethod
    def cleanup(self):
        """
        Cleanup this ModelStateWriter
        """

    def put_file(self, local_file_path: str, asset_file_path: Optional[str] = None):
        """
        Copy and put a single file from local disk to a
        specified path in the backing store
        :param local_file_path: Local file to be copied
        :param asset_file_path: Filepath in the backing
            store to copy the file to. By default, the
            file will be copied to the root path of
            the backing store.
        """
        if not asset_file_path:
            asset_file_path = os.path.basename(local_file_path)
        with self.open(asset_file_path) as fp_out:
            with open(local_file_path, "rb") as local_file:
                shutil.copyfileobj(local_file, fp_out)

    def put_directory(self, local_root_path: str, asset_root_path: str = ""):
        """
        Copy and put a directory from local disk to a
        specified path in the backing store
        """
        for dir_name, _, files in os.walk(local_root_path):
            rel_dir = os.path.relpath(dir_name, local_root_path)
            for file_name in files:
                self.put_file(os.path.join(dir_name, file_name), os.path.join(asset_root_path, rel_dir, file_name))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class TemporaryDirectoryBackedModelStateWriter(ModelStateWriter):
    """
    Writes a model version's state to a temporary directory
    """

    def __init__(self, tmp_dir: Optional[TemporaryDirectory] = None):
        self.tmp_dir = tmp_dir if tmp_dir else TemporaryDirectory()

    def mkdir(self, asset_dir_path: str) -> str:
        new_dir_path = os.path.join(self.tmp_dir.name, asset_dir_path)
        os.makedirs(new_dir_path, exist_ok=True)
        return new_dir_path

    @contextmanager
    def open(self, asset_file_path: str, mode: str = "wb") -> Iterator[IO]:
        """
        Open a file in this ModelStateWriter's
        temporary directory for writing.
        """
        os.makedirs(
            os.path.dirname(os.path.join(self.tmp_dir.name, asset_file_path)),
            exist_ok=True,
        )
        with open(os.path.join(self.tmp_dir.name, asset_file_path), mode) as backing_file:
            yield backing_file

    def cleanup(self):
        """
        Cleanup this ModelStateWriter's
        temporary directory
        """
        self.tmp_dir.cleanup()


class SubdirectoryModelStateWriter(ModelStateWriter):
    """
    Writes files to model state in a named subdirectory. This is useful for
    custom serializers, as it ensures that they write to a unique directory
    per argument. The writer does not protect against writing outside the
    subdirectory (e.g. by using `..` in the path).

    Example use:
    ```
    writer = SubdirectoryModelStateWriter("dir", backing_writer)
    with writer.open("model.json", "wb") as f:
        f.write(model)
    ```
    This will create a file at `dir/model.json` in the backing store.
    """

    def __init__(self, subdir: str, writer: ModelStateWriter):
        self.subdir = subdir
        self.writer = writer

    @contextmanager
    def open(self, asset_file_path: str, mode: str = "wb") -> Iterator[IO]:
        with self.writer.open(os.path.join(self.subdir, asset_file_path), mode) as asset_file:
            yield asset_file

    def mkdir(self, asset_dir_path: str) -> str:
        return self.writer.mkdir(os.path.join(self.subdir, asset_dir_path))

    def cleanup(self):
        self.writer.cleanup()
