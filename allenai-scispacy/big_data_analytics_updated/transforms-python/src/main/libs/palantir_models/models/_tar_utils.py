#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import os
import tarfile

# model weights layers are packaged as `app/data/model/` but `/app/data/model` is also valid
_VALID_ROOT_PATHS = ["/app/data/model/", "app/data/model/"]


def _extract_model_layer_tarfiles(source_directory: str, destination_path: str) -> None:
    """
    Extracts the tarfiles in `source_directory`, merges them, and writes the output `destination_path`
    Tarfiles are expected to have a rootpath of /app/data/model or app/data/model (with or without leading slash)
    """
    tar_files = []
    for root, dirs, files in os.walk(source_directory):
        for filename in files:
            if filename.lower().endswith(".tar.gz"):
                tar_files.append(os.path.join(root, filename))

    for tar_file_path in tar_files:
        with tarfile.open(tar_file_path, "r:gz") as tar:
            for member in tar.getmembers():
                _extract_member_if_valid(tar, member, destination_path)


# extracts the tar member to path specified
# relativizes the path such that something at /app/data/model/something is extracted to {path_to_extract}/something
def _extract_member_if_valid(tar: tarfile.TarFile, member: tarfile.TarInfo, path_to_extract: str):
    for path in _VALID_ROOT_PATHS:
        if member.path.startswith(path):
            member.path = os.path.relpath(member.path, path)
            tar.extract(member, path=path_to_extract)
