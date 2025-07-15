#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from inspect import signature, Parameter
from functools import wraps
from typing import cast, List, Tuple, Iterable, Union, Optional, Callable, TypeVar
from pyspark.sql import functions as F, types as T, Column

R = TypeVar("R", Column, Iterable[Column], Union[Column, Iterable[Column]])


def column_function(func: Callable[..., R]) -> Callable[..., R]:
    """A decorator for functions that take one or more columns as input and output one or more columns.

    String values will be interpreted as column names and implicitly coerced to :class:`Column` objects prior to
    passing them to the wrapped function - this way the wrapped function works the same as :module:`pyspark.sql`
    functions, from the user's perspective, but without having to implement logic to handle the varying input types.

    The coercion works even with lists or tuples (it will coerce those into tuples of :class:`Column` objects).

    :class:`Column` objects can also be given directly, and they will be passed along untouched. None is also propagated
    unmodified.

    If any other type is detected, the wrapper attempts to convert it to a literal column.

    However, if the wrapped function annotates types for its arguments, and those types are not :class:`Column` (or a
    list or tuple of those), they will not be coerced. This is useful as a mechanism for accepting non-column arguments
    that alter the function's global behaviour.

    The wrapped function result will be aliased to its call signature (with bound arguments). If it returns multiple
    columns, the alias will be appended with the output index (i.e. '#0', '#1' etc.). As a special case, if the function
    takes a single column name as input and returns a single column, the output is aliased to have the same name as the
    input.

    :param func: Function that takes one or more columns as input and output one or more columns.
    :type func: Callable

    :return: Wrapped function.
    :rtype: Callable
    """

    def coerce_column(value):
        if value is None or isinstance(value, Column):
            return value
        if isinstance(value, str):
            return F.col(value)
        if isinstance(value, (tuple, list)):
            return tuple(coerce_column(subvalue) for subvalue in value)
        return F.lit(value)

    def is_ignorable(annotation):
        if annotation in (Parameter.empty, Column, Optional[Column]):
            return False
        # Maintainer: be mindful of Python 3.7+, where __origin__ is redefined from a type (List) to a class (list)
        if (
            getattr(annotation, "__origin__", None) in (List, Tuple, list, tuple)
            and annotation.__args__
        ):
            return max(
                is_ignorable(subtype)
                for subtype in annotation.__args__
                if subtype is not Ellipsis
            )
        return True

    def is_sort_expression(result):
        return (
            result._jc.expr().getClass().toString()
            == "class org.apache.spark.sql.catalyst.expressions.SortOrder"
        )

    @wraps(func)
    def wrapper(*args, **kwargs) -> R:
        sig = signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        non_ignored_params = [
            param_name
            for param_name, param in sig.parameters.items()
            if not is_ignorable(param.annotation)
        ]
        non_ignored_values = [
            bound_args.arguments[param_name] for param_name in non_ignored_params
        ]

        for param_name in non_ignored_params:
            bound_args.arguments[param_name] = coerce_column(
                bound_args.arguments[param_name]
            )

        result = func(*bound_args.args, **bound_args.kwargs)

        if (
            isinstance(result, Column)
            and len(non_ignored_values) == 1
            and not is_sort_expression(result)
        ):
            value = non_ignored_values[0]
            if isinstance(value, str):
                return result.alias(value)
            else:
                return result.alias(f"{func.__name__}({value})")
        elif isinstance(result, Column):
            return result
        elif isinstance(result, Iterable):
            if any(not isinstance(col, Column) for col in result):
                raise RuntimeError(
                    f"{func.__name__} returned something which is not a column"
                )
            func_name = (
                f"{func.__name__}({', '.join(str(x) for x in non_ignored_values)})"
            )
            # cast is needed due to a bug in Pyright: https://github.com/microsoft/pyright/issues/7065
            return cast(
                R,
                tuple(
                    col.alias(f"{func_name}#{i}")
                    if not is_sort_expression(col)
                    else col  # sort expressions don't support aliases
                    for i, col in enumerate(result)
                ),
            )
        else:
            raise RuntimeError(
                f"{func.__name__} returned something that is not a column or iterable of columns: {result}"
            )

    return wrapper


def typed_empty_array(element_type):
    """
    A function which produces an empty array using the provided datatype, with spark2 support.

    :param element_type: DataType of the empty array to be created.
    :type element_type: DataType

    :return: Empty ArrayType object with DataType `element_type`.
    :rtype: ArrayType
    """
    if isinstance(element_type, T.StringType):
        return F.array()  # this creates an array<string>, casting not allowed

    # This is less efficient but more flexible
    return F.from_json(F.lit("[]"), schema=T.ArrayType(element_type))
