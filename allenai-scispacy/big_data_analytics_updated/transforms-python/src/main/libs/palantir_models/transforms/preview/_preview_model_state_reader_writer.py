#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import os
import shutil
from contextlib import contextmanager
from typing import IO, Iterator

from ...models._state_accessors import ModelStateReader, ModelStateWriter


class PreviewFileSystemBackedModelStateReader(ModelStateReader):
    """
    Retrieve a model version's model.zip from src path to dst path in local filesystem.
    """

    def __init__(self, files_dir: str):
        super().__init__()
        self._files_dir = files_dir

    def extract(self, destination_path: str) -> None:
        os.makedirs(destination_path, exist_ok=True)
        shutil.copytree(self._files_dir, destination_path, dirs_exist_ok=True)


class PreviewFileSystemBackedModelStateWriter(ModelStateWriter):
    """
    Writes model state files to a directory and does not clean up.
    """

    def __init__(self, files_dir: str):
        self._files_dir = files_dir

    @contextmanager
    def open(self, asset_file_path: str, mode: str = "wb") -> Iterator[IO]:
        os.makedirs(
            os.path.dirname(os.path.join(self._files_dir, asset_file_path)),
            exist_ok=True,
        )
        with open(
            os.path.join(self._files_dir, asset_file_path),
            mode,
        ) as backing_file:
            yield backing_file

    def mkdir(self, asset_dir_path: str) -> str:
        path = os.path.join(self._files_dir, asset_dir_path)
        os.makedirs(path, exist_ok=True)
        return path

    def cleanup(self):
        # no need to cleanup for preview
        pass
