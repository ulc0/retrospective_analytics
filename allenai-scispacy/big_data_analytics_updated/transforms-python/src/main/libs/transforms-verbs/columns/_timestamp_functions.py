#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from pyspark.sql import functions as F, Column
from typing import Optional
from ._utils import column_function


@column_function
def timestamp_diff(start, end=None, unit: Optional[str] = "seconds") -> Column:
    """
    Returns the amount of time from start to end.
    Start and end are expected :class:`Column` objects or column names, with `end` defaulting to the current time.
    `unit` specifies the time unit, and must be one of 'seconds', 'minutes', 'hours', 'days', 'weeks', 'months', or
    'years'.

    :param start:
        TimestampType column name or Column object with starting moment of the window.
    :type start: Col or str
    :param end:
        TimestampType column name or Column object with ending moment of the window. Defaults to the current time.
    :type end: Col or str or None
    :param unit: Unit of time for the numerical result. Defaults to seconds.
    :type unit: str, optionoal

    :return: A numerical column indicating a span of time in `unit` units. Negative if start > end.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> from datetime import datetime
        >>> df = spark_session.createDataFrame([
                {
                    "start": datetime(2020, 1, 1, 0),
                    "end": datetime(2020, 1, 1, 2)
                },
                {
                    "start": datetime(2020, 1, 1, 2),
                    "end": datetime(2020, 1, 1, 0)
                },
            ])
        >>> df = df.withColumn("diff", C.timestamp_diff("start", "end"))
        >>> df.show()
        +------------------------+------------------------+-------+
        |                   start|                     end|   diff|
        +------------------------+------------------------+-------+
        |2020-01-01T02:00:00.000Z|2020-01-01T03:00:00.000Z|   7200|
        +------------------------+------------------------+-------+
        |2020-01-01T03:00:00.000Z|2020-01-01T02:00:00.000Z|  -7200|
        +------------------------+------------------------+-------+
    """
    if end is None:
        end = F.current_timestamp()
    divisors = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
        "months": 2592000,
        "years": 31536000,
    }
    return (
        end.cast("timestamp").cast("double") - start.cast("timestamp").cast("double")
    ) / divisors[unit]


def _time_unit_name_to_value(unit: str) -> int:
    """
    Converts an SI unit string to a multiplier between seconds and the unit.

    :param unit: String of a know SI prefix (eg. s, ms, us).
    :type unit: str

    :return: Conversion multiplier between seconds and the unit.
    :rtype: int
    """
    if unit is None or unit in ("s", "sec", "secs", "seconds", "second"):
        return 1
    if unit in ("ms", "milli", "millis", "milliseconds", "millisecond"):
        return 1_000
    if unit in ("us", "μs", "micro", "micros", "microseconds", "microsecond"):
        return 1_000_000
    raise ValueError("Unit must be a known SI prefix (s, ms, us).")


@column_function
def unix_to_timestamp(col, unit: Optional[str] = "seconds") -> Column:
    """
    Converts a UNIX timestamp to a native timestamp, with optional input unit.

    :param col: A UNIX timestamp (LongType) column name or column object.
    :type col: Col or str
    :param unit: String indicating which time unit to use for the UNIX time. Defaults to seconds.
    :type unit: str, optional

    :return: TimestampType column of native timestamps.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = spark_session.createDataFrame([('1616927583',), ('1602784845',)], ['unix'])
        >>> df = df.withColumn("timestamp", C.unix_to_timestamp("unix"))
        >>> df.show()
        +----------+------------------------+
        |      unix|               timestamp|
        +----------+------------------------+
        |1616927583|2021-03-28T10:33:03.000Z|
        |1602784845|2020-10-15T18:00:45.000Z|
        +----------+------------------------+
    """
    return (col / _time_unit_name_to_value(unit)).cast("timestamp")


@column_function
def timestamp_to_unix(col, unit: Optional[str] = "seconds") -> Column:
    """Converts a native timestamp column into a UNIX time, with optional output unit.

    :param col: TimestampType column name or Column object.
    :type col: Col or str
    :param unit: String indicating which time unit to use for the UNIX time. Defaults to seconds.
    :type unit: str, optional

    :return: A LongType Column containing UNIX timestamps.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> from datetime import datetime
        >>> df = spark_session.createDataFrame([(datetime(2020, 1, 1, 0),)], ['timestamp'])
        >>> df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        >>> df = df.withColumn("unix", C.timestamp_to_unix("timestamp"))
        >>> df.show()
        +------------------------+----------+
        |               timestamp|      unix|
        +------------------------+----------+
        |2020-01-01T00:00:00.000Z|1577836800|
        +------------------------+----------+
    """
    return (col.cast("double") * _time_unit_name_to_value(unit)).cast("long")


@column_function
def window_overlap_duration(
    start_one, end_one, start_two, end_two, unit: Optional[str] = "seconds"
) -> Column:
    """Computes the span of time of the overlap between two windows.

    Nulls are interpreted as open ends. Zero overlap indicates one of the ends coincides exactly. Negative values
    indicate that there is no overlap (the negative amount represents the interval between one window's end the other's
    start).

    :param start_one: TimestampType column name or Column object with starting moment of the first window.
    :type start_one: Col or str
    :param end_one: TimestampType column name or Column object with ending moment of the first window.
    :type end_one: Col or str
    :param start_two: TimestampType column name or Column object with starting moment of the second window.
    :type start_two: Col or str
    :param end_two: TimestampType column name or Column object with ending moment of the second window.
    :type end_two: Col or str
    :param unit: Unit of time for the numerical result. Defaults to seconds.
    :type unit: str, optional

    :return: A numerical column indicating a span of time in `unit` units.
    :rtype: Column

    :Example Usage:
    ::

        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> from datetime import datetime
        >>> df = spark_session.createDataFrame([
                {
                    "start1": datetime(2020, 1, 1, 0),
                    "end1": datetime(2020, 1, 1, 2),
                    "start2": datetime(2020, 1, 1, 1),
                    "end2": datetime(2020, 1, 1, 3),
                },
            ])
        >>> df = df.withColumn("overlap", C.window_overlap_duration("start1", "end1", "start2", "end2"))
        >>> df.show()
        +------------------------+------------------------+------------------------+------------------------+-------+
        |                  start1|                    end1|                  start2|                    end2|overlap|
        +------------------------+------------------------+------------------------+------------------------+-------+
        |2020-01-01T02:00:00.000Z|2020-01-01T03:00:00.000Z|2020-01-01T00:00:00.000Z|2020-01-01T01:00:00.000Z|   3600|
        +------------------------+------------------------+------------------------+------------------------+-------+
    """
    start = F.greatest(start_one, start_two)
    end = F.least(end_one, end_two)
    return timestamp_diff(start, end, unit=unit)
