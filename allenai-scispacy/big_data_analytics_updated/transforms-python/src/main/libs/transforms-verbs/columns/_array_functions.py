#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import functools
from typing import Callable, TypeVar, Optional

from pyspark.sql import functions as F, types as T, Column

from ._utils import column_function, typed_empty_array

DataType = TypeVar("DataType", bound=T.DataType)


@column_function
def array_union(*columns, element_type: Optional[DataType] = T.StringType()) -> Column:
    """Computes the union of a sequence of arrays, ignoring any nulls.

    Note: duplicated elements are removed.

    By default, assumes the element type is a string, but this can be overriden with the `element_type` argument.

    :param columns: Variable length list of columns of type ArrayType to be unioned.
    :type columns: Col
    :param element_type: DataType of the columns to be reduced. Defaults to StringType.
    :type element_type: DataType, optional

    :return: Column with an array of distinct elements from the unioned columns
    :rtype: Col

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F, types as T
        >>> from transforms.verbs import columns as C
        >>> data = [(["a", "b"], ["a", "c", "c"], ["d"])]
        >>> df = spark_session.createDataFrame(data, ("c1", "c2", "c3"))
        >>> df = df.withColumn("c4", C.array_union(F.col("c1"), F.col("c2"), F.col("c3")))
        >>> df.show()
        +------+---------+---+------------+
        |    c1|       c2| c3|          c4|
        +------+---------+---+------------+
        |[a, b]|[a, c, c]|[d]|[a, b, c, d]|
        +------+---------+---+------------+
    """
    return reduce_array_columns(F.array_union, *columns, element_type=element_type)


@column_function
def array_except(*columns, element_type: Optional[DataType] = T.StringType()) -> Column:
    """Computes the chained difference of a sequence of arrays, ignoring any nulls.

    Using set algebra, for arrays A, B, C... this is equivalent to A - B - C...

    Note: duplicated elements are removed.

    By default, assumes the element type is a string, but this can be overriden with the `element_type` argument.

    :param columns: Variable length list of columns of type ArrayType.
    :type columns: Col
    :param element_type: DataType of the columns to be reduced. Defaults to StringType.
    :type element_type: DataType, optional

    :return: Column with a distinct array of the elements resulting from the difference.
    :rtype: Col

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from pyspark.sql import types as T
        >>> from transforms.verbs import columns as C
        >>> data = [(["a", "b", "c"], ["a", "e"], ["d", "c"])]
        >>> df = spark_session.createDataFrame(data, ("c1", "c2", "c3"))
        >>> df = df.withColumn("c4", C.array_except(F.col("c1"), F.col("c2"), F.col("c3")))
        >>> df.show()
        +---------+------+------+---+
        |       c1|    c2|    c3| c4|
        +---------+------+------+---+
        |[a, b, c]|[a, e]|[d, c]|[b]|
        +---------+------+------+---+
    """
    return reduce_array_columns(F.array_except, *columns, element_type=element_type)


@column_function
def reduce_array_columns(
    function: Callable[[Column, Column], Column],
    *columns,
    element_type: Optional[DataType] = T.StringType()
) -> Column:
    """Reduces a sequence of array columns using the given binary array function, ignoring any nulls.

    The function is first applied to the first two columns in the sequence, and from then onwards the output of the last
    application is used as the left input to the binary function, and the next column as the right input.
    This process is repeated until there are no columns left, and the final resulting array is then returned.

    For example, given the sequence of columns A, B, C, D and the function F, this is equivalent to:
    F(F(F(F(A, B), C), D).

    Any nulls are interpreted as empty arrays.

    By default, assumes the element type is a string, but this can be overriden with the `element_type` argument.

    :param function: A binary array function to be used for reducing ArrayType columns.
    :type function: Callable[[Col, Col], Col]
    :param columns: Variable length list of columns of type ArrayType to be reduced.
    :type columns: Col
    :param element_type: DataType of the columns to be reduced. Defaults to StringType.
    :type element_type: DataType, optional

    :return: ArrayType Column, the result of the reducing function.
    :rtype: Col

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from pyspark.sql import types as T
        >>> from transforms.verbs import columns as C
        >>> data = [(["a", "b"], ["c", "d", "e"], ["f"])]
        >>> df = spark_session.createDataFrame(data, ("c1", "c2", "c3"))
        >>> df = df.withColumn("c4", C.reduce_array_columns(F.array_union, F.col("c1"), F.col("c2"), F.col("c3")))
        >>> df.show()
        +------+---------+---+-------------------+
        |    c1|       c2| c3|                 c4|
        +------+---------+---+-------------------+
        |[a, b]|[c, d, e]|[f]|[a, b, c, d, e, f ]|
        +------+---------+---+-------------------+

    """
    array_template = typed_empty_array(element_type)
    coalesced_arrays = (F.coalesce(column, array_template) for column in columns)
    return functools.reduce(function, coalesced_arrays)


@column_function
def collect_array_elements_set(column) -> Column:
    """
    Aggregation function that collects distinct elements in an ArrayType column.

    :param column: ArrayType column name or Column object.
    :type column: Col or str

    :return: ArrayType Column with a single row containing distinct elements.
    :rtype: Col

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = spark_session.createDataFrame([([1, 2, 3, 2],), ([4, 5, 5, 4],)], ["data"])
        >>> df.show()
        +------------+
        |        data|
        +------------+
        |[1, 2, 3, 2]|
        |[4, 5, 5, 4]|
        +------------+
        >>> df = df.select(C.collect_array_elements_set("data").alias("collected"))
        >>> df.show()
        +----------------+
        |       collected|
        +----------------+
        |[ 1, 2, 3, 4, 5]|
        +----------------+
    """
    return F.array_distinct(F.flatten(F.collect_list(column)))
