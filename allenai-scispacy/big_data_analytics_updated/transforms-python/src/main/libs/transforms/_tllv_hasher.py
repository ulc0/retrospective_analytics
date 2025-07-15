import collections
import hashlib
import inspect
import logging
import os
from io import TextIOWrapper
from types import ModuleType
from typing import Dict, NamedTuple, Optional, Sequence, cast

log = logging.getLogger(__name__)


class HashException(Exception):
    """Raised when fail to hash object or files."""


def hash_object(obj: object, source_dir: str) -> str:
    """Computes object hash. Must return different hash if behaviour of this object is changed in any way.

    Args:
        obj (object): Object to hash.
        source_dir (str): Directory with user source. Modules outside of this directory are ignored.
            Module where object to hash comes from must be inside this directory.

    Returns:
        str: Hash value of object. Hex string.

    Raises:
        HashException: If failed to hash. Either we're unable to resolve some module or we were not able to find
            object's module inside project dir.
    """
    module = inspect.getmodule(obj)

    try:
        hashed_sources = _bfs_hash_modules(
            cast(ModuleType, module), os.path.normpath(source_dir) + os.path.sep
        )
    except _ResolveModuleException as exc:
        log.exception("Failed to resolve module")
        raise HashException("Failed to hash object") from exc

    hasher = hashlib.md5()
    for _, hashed_source in sorted(hashed_sources.items()):
        hasher.update(hashed_source)
    return hasher.hexdigest()


def hash_requirements(requirements_file: TextIOWrapper, package_name: str) -> str:
    """Hash requirements from Conda requirements.lock file. Skip user code package version.

    Args:
        requirements_file (file): File with requirements created with Conda.
        package_name (str): User package name.

    Returns:
        str: Hash value of requirements. Hex string.
    """
    hasher = hashlib.md5()
    if not requirements_file:
        return hasher.hexdigest()
    lines = requirements_file.readlines()
    for line in lines:
        if line.startswith("#") or line.startswith(package_name + "="):
            continue
        hasher.update(line.encode("utf-8"))
    return hasher.hexdigest()


def hash_files(paths: Sequence[str]) -> str:
    """
    Hash content of files with given paths. Order of paths does not impact hash. Assume paths are
    valid and it is possible to read from all of them.

    Args:
        paths (list of str): List of absolute file paths whose content should be hashed.

    Returns:
        str: Combined hash of all files. Hex string.
    """
    hasher = hashlib.md5()
    for path in sorted(paths):
        with open(path, "rb") as f:
            content = f.read()
            hasher.update(hashlib.md5(content).digest())
    return hasher.hexdigest()


class _ResolveModuleException(Exception):
    """Raised when fail to resolve module to source file."""


class _ResolvedModule(NamedTuple):
    module: ModuleType
    file_path: str
    source: bytes


def _resolve_module(module: ModuleType, search_dir: str) -> Optional[_ResolvedModule]:
    """Returns _ResolvedModule or None if module is built-in module or outside of search dir.

    Args:
        module (module): Module object to be resolved.
        search_dir (str): Only resolve module if it's inside this directory.

    Returns:
        _ResolvedModule: The module with path and source.

    Raises:
        _ResolveModuleException: If it's not able to either find module file path or py/pyc source.
    """
    try:
        # Resolve module to source file
        file_path = inspect.getsourcefile(module)
        if not file_path:
            # If failed, resolve to file
            file_path = inspect.getfile(module)
    except TypeError:
        # This is built-in module
        return None

    if not file_path:
        # Unable to resolve module to file
        raise _ResolveModuleException(f"Failed to get file for module {module}")

    if not file_path.startswith(search_dir):
        # Module is outside of search directory
        return None

    if not file_path.endswith(".py") and not file_path.endswith(".pyc"):
        # If resolved to something other than py or pyc, it's not safe to skip file so we fail
        raise _ResolveModuleException(
            f"Failed to handle file, unexpected extension: {file_path}"
        )

    try:
        with open(file_path, "rb") as f:
            # If resolved to pyc, drop first 8 bytes to skip four-byte magic number and four-byte modification timestamp
            if file_path.endswith(".pyc"):
                f.read(8)
            source = f.read()
    except Exception as e:
        raise _ResolveModuleException(
            f"Failed to get source for source file {file_path}: {e}"
        ) from e

    return _ResolvedModule(module, file_path, source)


def _visit_module(module: ModuleType, search_dir: str) -> Dict[str, _ResolvedModule]:
    """Collects and resolves all modules referenced in this one.

    Args:
        module (module): Module to be visited.
        search_dir (str): Only return module if it's inside this directory.

    Returns:
        dict of (str, _ResolvedModule): Map from path to _ResolvedModule with that path.
    """
    descendants: Dict[str, _ResolvedModule] = {}
    for attribute_name in dir(module):
        attribute = getattr(module, attribute_name)
        # This returns None if can't find module in which case we just skip it. It's not user module.
        child_module = inspect.getmodule(attribute)
        if child_module:
            resolved_module = _resolve_module(child_module, search_dir)
            if resolved_module and resolved_module.file_path not in descendants:
                descendants[resolved_module.file_path] = resolved_module
    return descendants


def _bfs_hash_modules(root: ModuleType, search_dir: str) -> Dict[str, bytes]:
    """Traverse modules and hash source for each module inside search directory.

    Args:
        root (module): Module to start search from.
        search_dir (str): Directory to limit search.

    Returns:
        dict of (str, bytes): Map from file path to hash of source code interpretable as a buffer of bytes.

    Raises:
        _ResolveModuleException: If it's not able to resolve all found modules or root module is outside of search
            directory.
    """
    resolved_root = _resolve_module(root, search_dir)
    if not resolved_root:
        raise _ResolveModuleException(
            f"Root module {root} resolved to None in search directory {search_dir}"
        )

    map_path_to_hash: Dict[str, bytes] = {
        resolved_root.file_path: hashlib.md5(resolved_root.source).digest()
    }
    queue = collections.deque([root])
    while queue:
        module = queue.popleft()
        descendants = _visit_module(module, search_dir)
        for path, descendant in descendants.items():
            if path not in map_path_to_hash:
                map_path_to_hash[path] = hashlib.md5(descendant.source).digest()
                queue.append(descendant.module)
    return map_path_to_hash
