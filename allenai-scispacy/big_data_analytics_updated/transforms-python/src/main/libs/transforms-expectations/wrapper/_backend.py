"""
Data expectations is compatible with either normal pyspark transforms
or with lightweight transforms. For lightweight transforms the chosen data engine is
Polars in lazy mode. We keep track of a global engine variable to help our abstractions
know when to operate on a pyspark dataframe or in a polars lazyframe
"""

from enum import Enum, auto


class Backend(Enum):
    SPARK = auto()
    POLARS = auto()


CURRENT_BACKEND = Backend.SPARK


def GET_CURRENT_BACKEND() -> Backend:
    return CURRENT_BACKEND


def SET_CURRENT_BACKEND(backend: Backend):
    global CURRENT_BACKEND  # pylint: disable=global-statement
    CURRENT_BACKEND = backend
