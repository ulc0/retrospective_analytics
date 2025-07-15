import inspect
import logging
import os
from typing import Any, Callable, Optional, Tuple, Union

from transforms import _env_utils

log = logging.getLogger(__name__)


def compute_source_provenance(
    compute_func: Callable[..., Any],
) -> Tuple[Union[str, None], Union[int, None]]:
    try:
        if _is_wrapped(compute_func):
            log.debug(
                "compute_func is wrapped, will use source provenance of inner function"
            )
            return _code_object_provenance(
                _unwrap(compute_func), _env_utils.ROOT_PROJECT_DIR
            )
        log.debug("compute_func is not wrapped, using stack-based source provenance")
        return _stack_source_provenance(
            _env_utils.PROJECT_SOURCE_DIR, _env_utils.ROOT_PROJECT_DIR
        )
    except ValueError as e:
        log.debug("Failed to compute stack source provenance: %s", e)
        # Fallback to taking source provenance off the compute function
        return _code_object_provenance(compute_func, _env_utils.ROOT_PROJECT_DIR)


def _is_wrapped(compute_func: Callable[..., Any]) -> bool:
    """Detects if a function has been wrapped by @functools.wraps."""
    return hasattr(compute_func, "__wrapped__")


def _unwrap(compute_func: Callable[..., Any]) -> Callable[..., Any]:
    """Recursively unwrap function."""
    if _is_wrapped(compute_func):
        return _unwrap(compute_func.__wrapped__)
    return compute_func


def stack_source_provenance(
    project_source_dir: Optional[str], root_project_dir: Optional[str]
):
    # Need to keep the method for back-compat reasons as used by changelog decorator (PDS-119308).
    return _stack_source_provenance(project_source_dir, root_project_dir)


def _stack_source_provenance(
    project_source_dir: Optional[str], root_project_dir: Optional[str]
) -> Tuple[str, Union[int, None]]:
    """Unpack frames until user module is found."""
    if not root_project_dir:
        raise ValueError("Project root dir not defined")
    root_dir = os.path.abspath(root_project_dir)
    if not project_source_dir:
        raise ValueError("Project source dir not defined")
    source_dir = os.path.normpath(os.path.abspath(project_source_dir)) + os.path.sep

    frame = inspect.currentframe()
    if not frame:
        raise ValueError(
            "Current frame is None. Stack frames might not be supported by interpreter"
        )

    while frame:
        try:
            filename = inspect.getsourcefile(frame)
            if filename and filename.startswith(source_dir):
                filename = os.path.relpath(filename, root_dir)
                lineno = frame.f_lineno
                assert lineno is None or isinstance(lineno, int)
                return filename, lineno
        except TypeError:
            # getsourcefile might throw, we don't want to fail on that
            pass
        # Go to previous frame
        frame = frame.f_back

    # No frame from user code found
    raise ValueError("Stack frame from user source code not found")


def code_object_provenance(
    compute_func: Callable[..., Any], root_project_dir: Optional[str]
):
    # Need to keep the method for back-compat reasons as used by changelog decorator (PDS-119308).
    return _code_object_provenance(compute_func, root_project_dir)


def _code_object_provenance(
    compute_func: Callable[..., Any], root_project_dir: Optional[str]
) -> Tuple[Union[str, None], int]:
    """Grab source provenance off the compute function's code object."""
    lineno = compute_func.__code__.co_firstlineno
    filename: Optional[str] = None

    if root_project_dir:
        root_dir = os.path.abspath(root_project_dir)
        filename = os.path.relpath(compute_func.__code__.co_filename, root_dir)

    return filename, lineno
