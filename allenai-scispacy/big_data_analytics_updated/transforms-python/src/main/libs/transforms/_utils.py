# Copyright 2017 Palantir Technologies, Inc.
import hashlib
import itertools
import sys
from functools import partial, wraps
from typing import Any, Callable, Dict, List, Mapping, Type, TypeVar

import decorator
import pyspark
from pyspark.sql import types as T

PIPELINE_ENTRYPOINT_OVERRIDE_KEY = "!pipelineEntrypointOverride!"


def is_spark_driver() -> bool:
    return not pyspark.files.SparkFiles._is_running_on_worker


_ReturnValue = TypeVar("_ReturnValue")


@decorator.decorator
def driver_only(
    func: Callable[..., _ReturnValue], *args: Any, **kwargs: Any
) -> _ReturnValue:
    if not is_spark_driver():
        raise ValueError("Function can only be called on the Spark driver.")

    return func(*args, **kwargs)


class SelectivePickleMixin(object):
    """Mixin that lets a class specify which fields to pickle."""

    def __getstate__(self):
        state = {}
        for field in self._PICKLE_FIELDS:
            state[field] = getattr(self, field)
        return state

    def __setstate__(self, state):
        for field, val in state.items():
            setattr(self, field, val)


class memoized_property(property):
    """A decorator for instance properties that caches the computed value on the instance.

    Deleting the attribute resets the property.
    """

    def __init__(self, func):
        """Wrap the given function as a property."""
        super().__init__(func)
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self._func = func

    def __get__(self, obj, cls):
        """Compute or retrieve the property's value.

        We cache the result of the property function on the object's `__dict__` object.
        """
        if obj is None:
            return self

        if self.__name__ not in obj.__dict__:
            obj.__dict__[self.__name__] = self._func(obj)

        return obj.__dict__[self.__name__]


class memoized_method(object):
    """A decorator for instance methods that cache the computed value on the instance."""

    def __init__(self, func):
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self._func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self._func

        try:
            cache = obj.__cache
        except AttributeError:
            cache = obj.__cache = {}  # pylint: disable=unused-private-member

        meth = partial(self, obj)

        # Create a clear method, on the method, so we can clear the cache
        def _clear():
            for func, args, kwargs in list(cache.keys()):
                if func == self._func:
                    del cache[(func, args, kwargs)]

        meth.clear = _clear

        return meth

    def __call__(self, *args, **kwargs):
        obj = args[0]
        cache = obj.__cache

        key = (self._func, args[1:], frozenset(kwargs.items()))

        if key not in cache:
            cache[key] = self._func(*args, **kwargs)

        return cache[key]


def memoized(function):
    """A decorator for memoizing function calls."""
    cache = {}

    @wraps(function)
    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        if key not in cache:
            cache[key] = function(*args, **kwargs)
        return cache[key]

    return wrapper


def iter_intersection(iter_a, iter_b, key=None):
    """Returns items of iter_a that also appear in iter_b.

    Iterators must be sorted by the 'key' function.

    Yields tuples of the two iterators deemed equal by 'key'
    """

    def _key(x):
        if key:
            return key(x)
        return x

    # with pep479 - in py37+ this is now a RuntimeError
    try:
        while True:
            item_a = next(
                iter_a
            )  # Base case is this line throws StopIteration # pylint: disable=stop-iteration-return

            iter_b = itertools.dropwhile(
                lambda item_b: _key(item_b) < _key(item_a), iter_b
            )
            item_b = next(
                iter_b
            )  # Or when this line throws StopIteration # pylint: disable=stop-iteration-return

            if _key(item_b) == _key(item_a):
                # Yield both items, because they might be different
                yield item_a, item_b
            elif _key(item_b) > _key(item_a):
                # Replace the item_b
                iter_b = itertools.chain([item_b], iter_b)
    except StopIteration:
        return


def iter_subtract(iter_a, iter_b, key=None):
    """Returns items of iter_a that do not appear in iter_b.
    Iterators must be sorted.
    """

    def _key(x):
        if key:
            return key(x)
        return x

    # with pep479 - in py37+ this is now a RuntimeError
    try:
        while True:
            item_a = next(
                iter_a
            )  # Base case is this line throws StopIteration # pylint: disable=stop-iteration-return
            iter_b = itertools.dropwhile(
                lambda item_b: _key(item_b) < _key(item_a), iter_b
            )

            try:
                item_b = next(iter_b)
                if _key(item_b) > _key(item_a):
                    yield item_a

                # Replace the item_b
                iter_b = itertools.chain([item_b], iter_b)
            except StopIteration:
                # No more items in b, all remaining items in A are valid
                yield item_a
                for item in iter_a:
                    yield item
    except StopIteration:
        return


def python_version_at_least(major: int, minor: int = 0, patch: int = 0) -> bool:
    """Checks Python version requirements."""
    return sys.version_info[:3] >= (major, minor, patch)


_Cls = TypeVar("_Cls")


def filter_instances(
    instance_map: Mapping[str, Any], cls: Type[_Cls]
) -> Dict[str, _Cls]:
    """Utility function for filtering map entries by the class of the value.

    Args:
        instance_map (dict of obj: obj): Map of instances to filter.
        cls (type): The type to filter for.

    Returns:
        (dict of obj: cls): The filtered dictionary.
    """
    return {k: v for k, v in instance_map.items() if isinstance(v, cls)}


def hash_list_ignore_order(items: List[str]):
    """Hashes list items in sorted order.

    Args:
        items (list of str): List of string to hash.

    Returns:
        str: Hex string of hashed sorted list.
    """
    hasher = hashlib.md5()
    for value in sorted(items):
        hasher.update(value.encode("utf-8"))
    return hasher.hexdigest()


def data_types_are_equal_ignoring_nullability(dt1: T.DataType, dt2: T.DataType) -> bool:
    """
    Compares the given schemas ignoring the nullability of the fields in the comparison.
    This is needed because of breaking changes in default nullability of some elements between spark versions.
    :return: true if the schemas are the same
    """
    if isinstance(dt1, T.ArrayType) and isinstance(dt2, T.ArrayType):
        return data_types_are_equal_ignoring_nullability(
            dt1.elementType, dt2.elementType
        )
    if isinstance(dt1, T.MapType) and isinstance(dt2, T.MapType):
        are_key_datatypes_equal = data_types_are_equal_ignoring_nullability(
            dt1.keyType, dt2.keyType
        )
        are_value_datatypes_equal = data_types_are_equal_ignoring_nullability(
            dt1.valueType, dt2.valueType
        )
        return are_key_datatypes_equal and are_value_datatypes_equal
    if isinstance(dt1, T.StructType) and isinstance(dt2, T.StructType):
        if len(dt1.fieldNames()) != len(dt2.fieldNames()):
            return False
        return all(
            map(
                lambda fields: data_types_are_equal_ignoring_nullability(*fields),
                zip(dt1.fields, dt2.fields),
            )
        )
    if isinstance(dt1, T.StructField) and isinstance(dt2, T.StructField):
        return (dt1.name == dt2.name) and data_types_are_equal_ignoring_nullability(
            dt1.dataType, dt2.dataType
        )
    return dt1 == dt2
