from ._backend import GET_CURRENT_BACKEND, Backend
from functools import wraps
import pyspark.sql.functions as F
import polars as pl
from ._wrapper import PolarsExpectationColumn


def delegate_if_in_spark_mode():
    """
    Every method signature in wrapper.functions is 100% compatible with the pyspark.sql.functions module.

    As such, if we are pyspark as the engine, we directly find the method with the same signature and directly call it.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if GET_CURRENT_BACKEND() == Backend.SPARK:
                spark_func = getattr(F, func.__name__)
                return spark_func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorator


def wrap_result():
    """
    Almost all function classes end up with creating a polars EXPR that we then wrap
    into the PolarsExpectationColumn object.

    As such, this decorator will do that wrapping automatically.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, pl.Expr):
                return PolarsExpectationColumn(result)
            return result

        return wrapper

    return decorator


def unwrap_col():
    """
    Unwrap the first argument if it is a PolarsExpectationColumn
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            new_args = list(args)
            if new_args and isinstance(new_args[0], PolarsExpectationColumn):
                new_args[0] = new_args[0].unwrap()
            return func(*new_args, **kwargs)

        return wrapper

    return decorator


def delegate_to_backend():
    """
    Applies three decorators that simplify the implementation of the functions in the wrapper.functions module.
    1. delegate_if_in_spark_mode: If we are in spark mode, we directly call the pyspark.sql.functions method.
    2. unwrap_col: Unwrap the first argument if it is a PolarsExpectationColumn, to give us the pl.EXPR object.
    3. wrap_result: Wrap the pl.EXPR result in a PolarsExpectationColumn object again.
    """

    def decorator(func):
        # Apply the decorators in the correct order
        func = delegate_if_in_spark_mode()(func)
        func = unwrap_col()(func)
        func = wrap_result()(func)
        return func

    return decorator
