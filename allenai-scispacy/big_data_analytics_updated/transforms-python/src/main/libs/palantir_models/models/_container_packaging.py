#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
import gzip
import hashlib
import io
import os
import tarfile
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, BinaryIO, ClassVar, List, Optional, Type, Union

if TYPE_CHECKING:
    from typing_extensions import Buffer


class ModelContainerConsts:
    """
    Constants defined in model-inference-base-image
    """

    DOCKER_WEIGHTS_NAME = "containerized-model"
    ADAPTER_PATH = "/app/data/adapter"
    MODEL_ENVIRONMENT_PATH = "/app/data/env"
    MODEL_PATH = "/app/data/model"
    SHA_256_DIGEST_PREFIX = "sha256:"


@dataclass
class LayerMetadata:
    """
    A dataclass representing metadata needed to upload a docker/OCI layer to a container registry.

    Attributes:
        tar_hash (str): Hexadecimal encoded hash of the uncompressed tarball.
        gzip_hash (str): Hexadecimal encoded hash of the gzipped tarball.
        gzip_size (int): Size in bytes of gzipped tarball.
    """

    SHA_256_DIGEST_PREFIX: ClassVar[str] = "sha256:"

    tar_hash: str
    gzip_hash: str
    gzip_size: int

    @property
    def tar_digest(self) -> str:
        """
        Digest of the uncompressed tarball (sha256:hash).
        """
        return self.SHA_256_DIGEST_PREFIX + self.tar_hash

    @property
    def gzip_digest(self) -> str:
        """
        Digest of the gzipped tarball (sha256:hash).
        """
        return self.SHA_256_DIGEST_PREFIX + self.gzip_hash


class LayerWriter:
    """
    Given an in-memory or in-disk output file, this class exposes methods to construct a docker/OCI layer manually.
    Clients are responsible for calling close() after finishing writing to the layer. After closing, the get()
    method can be called to obtain metadata needed to build a layer manifest or upload the layer to a registry.
    """

    def __init__(self, output_file: BinaryIO):
        self.hashed_gzip_output = HashingCountingIO(output_file)
        self.gzip_output = gzip.GzipFile(fileobj=self.hashed_gzip_output, mode="wb")
        self.hashed_tar_output = HashingCountingIO(self.gzip_output)
        self.tar_output = tarfile.open(fileobj=self.hashed_tar_output, mode="w|")

        # Tracks whether close() has been invoked. We cannot generate hashes or finalize
        # size until the writer has been closed.
        self.closed = False

    def add_fileobj(self, result_path: str, size: int, fileobj: BinaryIO) -> None:
        """
        Adds a file object with the specified size at exactly result_path in the layer.
        Trailing paths with empty size will be interpreted as folders.
        """
        assert not self.closed, "Cannot add files after writer has been closed"
        if result_path.endswith("/"):
            assert size == 0, "Directories must have size 0"

        tar_info = tarfile.TarInfo(name=result_path)
        self.canonicalize_tar_entry(tar_info, size)
        self.tar_output.addfile(tar_info, fileobj=fileobj)

    def add_file(self, result_path: str, source_path: str) -> None:
        """
        Copies source_path as a file at exactly result_path in the layer. Only files may be provided.
        """
        assert os.path.isfile(source_path), "Only files can be added with add_file"
        with open(source_path, "rb") as source_file:
            self.add_fileobj(result_path, os.path.getsize(source_path), source_file)

    def add_files(self, result_path: str, source_path: str) -> None:
        """
        Copies files recursively from source_path to result_path in the layer. Only directories may be provided.
        """
        assert os.path.isdir(source_path), "Only directories can be added with add_files"
        for subdir, _, files in os.walk(source_path):
            for file in files:
                full_path = os.path.join(subdir, file)
                relative_path = os.path.relpath(full_path, source_path)
                self.add_file(os.path.join(result_path, relative_path), full_path)

    def add_data(self, result_path: str, data: bytes) -> None:
        """
        Adds bytes data as a file at exactly result_path in the layer.
        Trailing paths with empty data will be interpreted as folders.
        """
        self.add_fileobj(result_path, len(data), io.BytesIO(data))

    def get(self) -> LayerMetadata:
        """
        Returns metadata needed to build a layer manifest or upload the layer to a registry.
        Must be called after close().
        """
        if not self.closed:
            raise ValueError("Cannot finalize layer until writer has been closed")
        return LayerMetadata(
            self.hashed_tar_output.hexdigest,
            self.hashed_gzip_output.hexdigest,
            self.hashed_gzip_output.count,
        )

    def close(self) -> None:
        """
        Closes the writer, finalizing the tar and gzip hashes.
        """
        if not self.closed:
            self.tar_output.close()
            self.gzip_output.close()
            self.closed = True

    @staticmethod
    def canonicalize_tar_entry(tar_info: tarfile.TarInfo, size: int) -> None:
        """
        Modify a tar entry, setting file attributes to constant values to ensure reproducibility.
        """
        tar_info.size = size
        tar_info.mtime = 0
        tar_info.uname = "foundry-ml"
        tar_info.gname = "foundry-ml"
        tar_info.uid = 5002
        tar_info.gid = 5002
        tar_info.mode = 0o777


class HashingCountingIO(BinaryIO):
    """
    Wraps an io/file-like object to compute the sha256 hash and count the number of bytes written.
    """

    def __init__(self, delegate: Union[io.IOBase, BinaryIO]):
        self.delegate = delegate
        self.hash_func = hashlib.sha256()
        self.total = 0

    @property
    def hexdigest(self) -> str:
        return self.hash_func.hexdigest()

    @property
    def count(self) -> int:
        return self.total

    # override write to count and hash
    def write(self, s: Union[bytes, "Buffer"]) -> int:
        count = self.delegate.write(s)
        self.hash_func.update(s)
        self.total += count
        return count

    def writelines(self, lines: "Union[Iterable[bytes], Iterable[Buffer]]") -> None:
        for line in lines:
            self.write(line)

    def writable(self) -> bool:
        return self.delegate.writable()

    # delegate other methods directly
    def __enter__(self) -> BinaryIO:
        self.delegate.__enter__()
        return self

    def __exit__(
        self,
        __type: Optional[Type[BaseException]],
        __value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        self.delegate.__exit__(__type, __value, __traceback)

    def __iter__(self) -> "Iterator[bytes]":
        raise NotImplementedError("Not readable")

    def __next__(self) -> bytes:
        raise NotImplementedError("Not readable")

    def close(self) -> None:
        self.delegate.close()

    def fileno(self) -> int:
        return self.delegate.fileno()

    def flush(self) -> None:
        self.delegate.flush()

    def isatty(self) -> bool:
        return self.delegate.isatty()

    def read(self, __n: int = -1) -> bytes:
        raise NotImplementedError("Not readable")

    def readable(self) -> bool:
        return False

    def readline(self, __limit: int = -1) -> bytes:
        raise NotImplementedError("Not readable")

    def readlines(self, __hint: int = -1) -> List[bytes]:
        raise NotImplementedError("Not readable")

    def seek(self, __offset: int, __whence: int = 0) -> int:
        raise NotImplementedError("Not seekable")

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise NotImplementedError("Not seekable")

    def truncate(self, __size: Optional[int] = None) -> int:
        raise NotImplementedError("Not seekable")


@contextmanager
def package_mice_weights_layer(state_writer_tmp_dir: str, output_dir=None, delete=True):
    """
    Packages the model weights written by the state writer to the tmp_dir into a docker layer

    Note: the layer file is not cleaned up automatically.
    """
    with tempfile.NamedTemporaryFile(
        prefix="model", suffix=".tar.gz", mode="w+b", dir=output_dir, delete=delete
    ) as layer_file:
        layer_writer = LayerWriter(layer_file)  # type: ignore
        layer_writer.add_files(ModelContainerConsts.MODEL_PATH, state_writer_tmp_dir)
        layer_writer.close()
        layer_metadata = layer_writer.get()
        layer_file.seek(0)

        yield layer_file, layer_metadata
