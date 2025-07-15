import logging
from enum import Enum

from conjure_python_client import ConjureHTTPError

from palantir_models.experiments._series import SeriesWriterThread

log = logging.getLogger("palantir_models.experiments")

EXPERIMENT_SIZE_OVERFLOW = "Models:ExperimentSizeOverflow"
EXPERIMENTS_ARE_DISABLED = "Models:ExperimentsAreDisabled"


class ErrorHandlerType(Enum):
    """
    Enum used to control how exceptions are handled. Exceptions may be thrown for invalid series types,
    experiment size overflows, etc.

    FAIL -> Rethrow any exceptions back to the caller.
    WARN -> Warn on the first exception.
    SUPPRESS -> Do not log anything.
    """

    FAIL = 1
    WARN = 2
    SUPPRESS = 3


class ExperimentException(Exception):
    """Experiment exception"""


class ExperimentErrorHandler:
    def __init__(self, error_handler_type: ErrorHandlerType, series_writer_thread: SeriesWriterThread):
        self.__error_handler_type = error_handler_type
        self.__thread = series_writer_thread

    def handle(self, exception: Exception):
        if self.__error_handler_type == ErrorHandlerType.FAIL:
            self.__thread.hard_close()
            msg = create_exception_message(exception)
            raise ExperimentException(msg) from exception

        if self.__error_handler_type == ErrorHandlerType.WARN:
            self.__thread.hard_close()
            msg, show_exception = create_exception_message(exception)
            msg = msg if not show_exception else f"{msg}\nException: {exception}"
            log.warning(msg)

        if self.__error_handler_type == ErrorHandlerType.SUPPRESS:
            self.__thread.hard_close()

        self.__thread.clear_exception()


# we only want to show user facing errors
def create_exception_message(error: Exception):
    if not isinstance(error, ConjureHTTPError):
        return "Experiment write failed due to unknown python error.", True

    if error.error_name == EXPERIMENT_SIZE_OVERFLOW:
        max_size = error.parameters["maximumSize"]
        return (
            f"Experiment size has reached the maximum allowed size of {max_size}. No more metrics will be written.",
            False,
        )

    if error.error_name == EXPERIMENTS_ARE_DISABLED:
        return "Experiments are disabled. Please contact an administrator to get experiment tracking enabled.", False

    return "Experiment write failed due to unknown network error.", True
