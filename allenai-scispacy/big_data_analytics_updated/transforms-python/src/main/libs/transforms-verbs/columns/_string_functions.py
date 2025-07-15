#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import unicodedata
import itertools
from typing import Union, Iterable
from pyspark.sql import functions as F, Column

from ._utils import column_function


@column_function
def trim_to_null(col) -> Column:
    """
    Trims trailing and leading whitespace, including newline characters,
    and sets to null instead of the empty string when the result is empty

    :param col: StringType column name or Column object.
    :type col: Column or str

    :return: StringType Column with trimmed leading and trailing whitespace.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = spark_session.createDataFrame([(" a",), ("b ",), ("  ",)], ["data"])
        >>> df = df.withColumn("trimmed", C.trim_to_null("data"))
        >>> df.show()
        +----+-------+
        |data|trimmed|
        +----+-------+
        |' a'|    'a'|
        |'b '|    'b'|
        |'  '|   null|
        +----+-------+
    """
    trimmed = F.regexp_replace(
        F.regexp_replace(col, r"[\p{Z}\s]+$", ""), r"^[\p{Z}\s]+", ""
    )
    return F.when(F.length(trimmed) == 0, None).otherwise(trimmed)


@column_function
def unicode_normalize(col) -> Column:
    """
    Returns the NFKC unicode normal form for the passed string column
    Note that the current implementation uses a UDF, so may not be performant
    for very large data.

    seealso https://docs.python.org/3/library/unicodedata.html#unicodedata.normalize

    :param col: StringType column name or Column object.
    :type col: Column or str

    :return: StringType Column with trimmed leading and trailing whitespace.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("\u2460",), ("C\u0327",), ("\u2075",)], ["data"])
        >>> df = df.withColumn("normalized", C.unicode_normalize("data"))
        >>> df.show()
        +----+----------+
        |data|normalized|
        +----+----------+
        | '①'|      '1'|
        | 'Ç'|       'Ç'|
        | '⁵́'|       '5́'|
        +----+----------+
    """

    def unicode_normalize_internal(string):
        if not string:
            return None
        return unicodedata.normalize("NFKC", string).strip()

    return F.udf(unicode_normalize_internal)(col)


@column_function
def empty_to_null(col, empty_value: Union[str, Iterable[str], None] = "") -> Column:
    """
    Returns null if the input matches the empty value(s), otherwise returns the string.

    :param col: StringType column name or Column object.
    :type col: Column or str
    :param empty_value: String(s) representing the value(s) that should be mapped to null. Defaults to an empty string.
    :type empty_value:  Union[str, Iterable[str]], optional

    :return: StringType Column where empty_value(s) are replaced with null.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([(" ",), ("b",), ("c",)], ["data"])
        >>> df = df.withColumn("result", C.empty_to_null("data", [" ", "X"]))
        >>> df.show()
        +----+------+
        |data|result|
        +----+------+
        | ' '|  null|
        | 'b'|   'b'|
        | 'X'|  null|
        +----+------+
    """
    if isinstance(empty_value, str):
        return F.when(col == empty_value, F.lit(None)).otherwise(col)
    if isinstance(empty_value, Iterable):
        return F.when(col.isin(*empty_value), F.lit(None)).otherwise(col)
    raise ValueError("empty_value must be a string or an iterable of strings.")


@column_function
def regexp_extract(col, pattern: str, group: Union[int, None] = 0) -> Column:
    """
    Extracts a regular expression from a column.
    This function is identical to the native `regexp_extract()` function, except that non-matches result in nulls
    instead of empty strings.

    :param col: StringType column name or Column object.
    :type col: Column or str
    :param pattern: Regular expression string.
    :type pattern: str
    :param group: Index of expression group to be extracted. Defaults to 0.
    :type group: int, optional

    :return: StringType Column containing the the regex extraction, or null in the case no match is found.
    :rtype: Column


    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("Checked by John",), ("Checked by Ann too",), ("err",)], ["data"])
        >>> df = df.withColumn("result", C.regexp_extract("data", r'(.)(by)(\\s+)(\\w+)', 4))
        >>> df.show()
        +--------------------+------+
        |                data|result|
        +--------------------+------+
        |   'Checked by John'|'John'|
        |'Checked by Ann too'| 'Ann'|
        |               'err'|  null|
        +--------------------+------+
    """
    match = F.regexp_extract(col, pattern, group)
    return F.when(match == "", F.lit(None).cast("string")).otherwise(match)


@column_function
def camel_to_snake_case(name) -> Column:
    """
    Converts identifiers in camelCase to snake_case.

    :param name: StringType column name or Column object.
    :type name: Column or str

    :return: StringType Column in snake case.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("fooBar",), ("var1",), ("barFooBar",)], ["camel"])
        >>> df = df.withColumn("snake", C.camel_to_snake_case("camel"))
        >>> df.show()
        +-----------+-----------+
        |      camel|      snake|
        +-----------+-----------+
        |   'fooBar'|  'foo_bar'|
        |     'var1'|     'var1'|
        |'barFooBar'|bar_foo_bar|
        +-----------+-----------+
    """
    sub1 = F.regexp_replace(name, r"(.)([A-Z][a-z]+)", r"$1_$2")
    return F.lower(F.regexp_replace(sub1, r"([a-z0-9])([A-Z])", r"$1_$2"))


@column_function
def snake_to_camel_case(name) -> Column:
    """
    Converts identifiers in snake_case to camelCase.

    :param name: StringType column name or Column object.
    :type name: Column or str

    :return: StringType Column in camel case.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("foo_bar",), ("var_1",), ("bar_foo_bar",)], ["snake"])
        >>> df = df.withColumn("camel", C.snake_to_camel_case("snake"))
        >>> df.show()
        +-------------+---------+
        |        snake|    camel|
        +-------------+---------+
        |    'foo_bar'| 'fooBar'|
        |      'var_1'|   'var1'|
        |'bar_foo_bar'|barFooBar|
        +-------------+---------+
    """
    words = F.regexp_replace(name, "_", " ")
    words = F.initcap(words)
    pascal_case = F.regexp_replace(words, " ", "")
    head = pascal_case.substr(1, 1)
    tail = pascal_case.substr(F.lit(2), F.length(pascal_case))
    return F.concat(F.lower(head), tail)


@column_function
def fullwidth_to_narrow(name, include_ideographic_space: bool = True) -> Column:
    """
    Converts East Asian fullwidth characters (wide character variants) to their naturally narrow (ASCII) forms.
    The conversions performed by this function are a strict subset of those performed by unicode_normalize,
    which also performs other conversions, such as ① to 1, that are not always desirable when working
    with East Asian text.
    Additionally, this function does not use a UDF, so it is much more performant than
    unicode_normalize is.
    See https://unicode.org/reports/tr11/ for documentation regarding East Asian width.

    :param name: StringType column name or Column object.
    :type name: Column or str
    :param include_ideographic_space: Whether ideographic space should be converted to ASCII space. Defaults to True.
    :type include_ideographic_space: bool

    :return: StringType Column with all East Asian wide character variants converted to ASCII.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("０１２７８９",), ("ＡＢＣａｂｃＸＹＺｘｙｚ",), ("コネコ",),
        >>>  ("！＄％＆＃（）＋－：＜＞＝？＠｛｝｜",), ("①⑨",)], ["fullwidth"])
        >>> df = df.withColumn("narrow", C.fullwidth_to_narrow("fullwidth"))
        >>> df.show()
        +---------------------------------+------------------------+
        |                       fullwidth|              narrow     |
        +---------------------------------+------------------------+
        |                     '０１２７８９'|                '012789'|
        |           'ＡＢＣａｂｃＸＹＺｘｙｚ'|          'ABCabcXYZxyz'|
        |                          'コネコ'|                 'コネコ'|
        | '！＄％＆＃（）＋－：＜＞＝？＠｛｝｜'|  '!$%&#()+-:<>=?@/{/}|'|
        |                          '①⑨ー'|                 '①⑨ー'|
        +---------------------------------+------------------------+
    """
    input_chars = (chr(0xFF01 + i) for i in range(94))
    result_chars = (chr(0x21 + i) for i in range(94))

    if include_ideographic_space:
        input_chars = itertools.chain([chr(0x3000)], input_chars)
        result_chars = itertools.chain([chr(0x20)], result_chars)

    return F.translate(
        name,
        "".join(input_chars),
        "".join(result_chars),
    )
