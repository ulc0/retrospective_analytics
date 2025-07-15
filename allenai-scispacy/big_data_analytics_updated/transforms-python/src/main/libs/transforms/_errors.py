# Copyright 2019 Palantir Technologies, Inc.
# pylint: disable=raising-format-tuple
import abc
import re
from typing import Optional

from future import utils


class ErrorCode(object):
    PERMISSION_DENIED = "PERMISSION_DENIED"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    REQUEST_ENTITY_TOO_LARGE = "REQUEST_ENTITY_TOO_LARGE"
    FAILED_PRECONDITION = "FAILED_PRECONDITION"
    INTERNAL = "INTERNAL"
    TIMEOUT = "TIMEOUT"
    CUSTOM_CLIENT = "CUSTOM_CLIENT"
    CUSTOM_SERVER = "CUSTOM_SERVER"


class ErrorType(object):
    """A Conjure ErrorType."""

    # pylint: disable-next=consider-using-f-string
    ERROR_NAME_PATTERN = re.compile("{0}:{0}".format(r"(([A-Z][a-z0-9]+)+)"))

    _error_code: Optional[str] = None
    _error_name: Optional[str] = None

    def __init__(self, error_code, error_name):
        self._error_code = error_code
        self._error_name = error_name
        self._check()

    @property
    def error_code(self):
        return self._error_code

    @property
    def error_name(self):
        return self._error_name

    def _check(self):
        if self.error_code not in dir(ErrorCode) or self.error_code.startswith("__"):
            raise TransformValueError("Unknown error code: %s", self.error_code)

        if not self.ERROR_NAME_PATTERN.match(self.error_name):
            raise TransformValueError(
                "Error name doesn't match Conjure pattern: %s", self.error_name
            )


class ConjureError(utils.with_metaclass(abc.ABCMeta)):
    """Abstract error type that returns an :class:`ErrorCode`."""

    @property
    @abc.abstractmethod
    def error_type(self) -> ErrorType:
        """Conjure error type."""
        raise NotImplementedError


class InvalidTransformReturnType(ConjureError, TypeError):
    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:InvalidTransformReturnType"
    )


class RequiredIncrementalTransform(ConjureError, ValueError):
    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:RequiredIncrementalTransform"
    )


class NoCorrespondingFunctionParameter(ConjureError, ValueError):
    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:NoCorrespondingFunctionParameter"
    )


class NotSupportedForTables(ConjureError, ValueError):
    error_type = ErrorType(
        ErrorCode.FAILED_PRECONDITION, "TransformsPython:NotSupportedForTables"
    )


def _unsafe_msg(safe_msg: str, *args: str) -> str:
    if not args:
        return safe_msg

    try:
        return safe_msg % args
    except:  # pylint: disable=bare-except
        # in case the provided arguments don't have the expected size
        return safe_msg


class TransformValueError(ValueError):
    """Wrapper for ValueError exception.

    Args:
        safe_msg (str): exception message with safe to log data.
        args: Variable length of unsafe args.
    """

    def __init__(self, safe_msg: str, *args: str) -> None:
        self.safe_msg = safe_msg
        super().__init__(_unsafe_msg(safe_msg, *args) or "Value Error")
        setattr(self, "__transform_internal_error", True)

    error_type = ErrorType(ErrorCode.INTERNAL, "TransformsPython:ValueError")


class TransformTypeError(TypeError):
    """Wrapper for TypeError exception.

    Args:
        safe_msg (str): exception message with safe to log data.
        args: Variable length of unsafe args.
    """

    def __init__(self, safe_msg: str, *args: str) -> None:
        self.safe_msg = safe_msg
        super().__init__(_unsafe_msg(safe_msg, *args) or "Type Error")
        setattr(self, "__transform_internal_error", True)

    error_type = ErrorType(ErrorCode.INTERNAL, "TransformsPython:TypeError")


class TransformKeyError(KeyError):
    """Wrapper for KeyError exception.

    Args:
        safe_msg (str): exception message with safe to log data.
        args: Variable length of unsafe args.
    """

    def __init__(self, safe_msg, *args):
        self.safe_msg = safe_msg
        super().__init__(_unsafe_msg(safe_msg, *args) or "Key Error")
        setattr(self, "__transform_internal_error", True)

    error_type = ErrorType(ErrorCode.INTERNAL, "TransformsPython:KeyError")


# We prevent retry for this error in PythonTransformsJobExecutor#shouldPreventRetry
class AbortedDueToFailedExpectation(ConjureError, ValueError):
    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:AbortedDueToFailedExpectation"
    )


# We prevent retry for this error in PythonTransformsJobExecutor#shouldPreventRetry
class AbortedDueToFailedExpectationWithException(ConjureError, ValueError):
    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT,
        "TransformsPython:AbortedDueToFailedExpectationWithException",
    )


class EntryPointError(ConjureError, KeyError):
    error_type = ErrorType(ErrorCode.INTERNAL, "TransformsPython:EntryPointError")


class OverloadedTransformFunction(ConjureError, TransformTypeError):
    error_type = ErrorType(
        ErrorCode.CUSTOM_CLIENT, "TransformPython:OverloadedTransformFunction"
    )


def is_conjure_error(exception):
    return isinstance(exception, ConjureError)


TRANSFORM_COMPUTE_ERROR_TYPE = ErrorType(
    ErrorCode.CUSTOM_CLIENT, "TransformsPython:TransformComputeError"
)
PY4J_JAVA_ERROR_TYPE = ErrorType(ErrorCode.INTERNAL, "TransformsPython:Py4jJavaError")
UNKNOWN_JOB_ERROR_TYPE = ErrorType(
    ErrorCode.INTERNAL, "TransformsPython:UnknownJobError"
)
PYARROW_IMPORT_ERROR_TYPE = ErrorType(
    ErrorCode.INTERNAL, "TransformsPython:PyArrowImportError"
)
PANDAS_IMPORT_ERROR_TYPE = ErrorType(
    ErrorCode.INTERNAL, "TransformsPython:PandasImportError"
)
