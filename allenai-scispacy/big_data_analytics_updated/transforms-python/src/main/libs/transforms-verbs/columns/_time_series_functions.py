#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
from pyspark.sql import functions as F, Column
from ._utils import column_function
from ._string_functions import trim_to_null, regexp_extract

from typing import Union
import re

_TIME_SERIES_SYNC_RID_PATTERN = r"ri\.time-series-catalog\.main\.sync\.([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})"  # noqa: E501 pylint: disable=line-too-long


@column_function
def qualified_series_id(series_id, sync_rid: Union[str, Column]) -> Column:
    # pylint: disable=line-too-long
    """
    Generates a Qualified Series ID JSON string for time series properties.
    The value of a time series property that is backed by multiple time series syncs must be a Qualified Series ID.

    A qualified series ID has the following shape:
    {"seriesId":"<series ID>","syncRid":"<sync RID containing this series ID>"}.

    Invalid sync RIDs or series IDs will coerce to Null.
    It is recommended to add an isNotNull expectation check on your series_id column.

    see also https://www.palantir.com/docs/foundry/time-series/time-series-concepts-glossary/#qualified-series-id

    :param series_id: StringType column name or Column object referencing the series ID.
    :type name: Column or str

    :param sync_rid: String value or StringType Column of the time series sync RID associated with the input series ID.
    :type name: Column or str

    :Example Usage:
    ::
        >>> from pyspark.sql import functions as F
        >>> from transforms.verbs import columns as C
        >>> df = ctx.spark_session.createDataFrame([("series_1",), ("series_2",), ("series_3",)], ["series_id"])
        >>> sync_rid = "ri.time-series-catalog.main.sync.aaaaaaaa-1111-2222-3333-bbbbbb444444"
        >>> df = df.withColumn("qualified_series_id", C.qualified_series_id("series_id", sync_rid)
        >>> df.show()
        +-----------+------------------------------------------------------------------------------------------------------------+
        |  series_id|                                                                                         qualified_series_id|
        +-----------+------------------------------------------------------------------------------------------------------------+
        | 'series_1'| '{"seriesId":"series_1","syncRid":"ri.time-series-catalog.main.sync.aaaaaaaa-1111-2222-3333-bbbbbb444444"}'|
        | 'series_2'| '{"seriesId":"series_2","syncRid":"ri.time-series-catalog.main.sync.aaaaaaaa-1111-2222-3333-bbbbbb444444"}'|
        | 'series_3'| '{"seriesId":"series_3","syncRid":"ri.time-series-catalog.main.sync.aaaaaaaa-1111-2222-3333-bbbbbb444444"}'|
        +-----------+------------------------------------------------------------------------------------------------------------+
    """  # noqa: E501
    # pylint: enable=line-too-long
    if isinstance(sync_rid, str):
        if not re.match(_TIME_SERIES_SYNC_RID_PATTERN, sync_rid):
            raise ValueError(
                f"Invalid time series catalog sync RID '{sync_rid}', "
                f"RID must be in format: {_TIME_SERIES_SYNC_RID_PATTERN}"
            )
        sync_rid_column = F.lit(sync_rid)
    elif isinstance(sync_rid, Column):
        sync_rid_column = regexp_extract(sync_rid, _TIME_SERIES_SYNC_RID_PATTERN)
    else:
        raise ValueError("Argument 'sync_rid' must be of type str or Column.")

    series_id_column = trim_to_null(series_id)

    return F.when(
        (series_id_column.isNotNull() & sync_rid_column.isNotNull()),
        F.to_json(
            F.struct(
                series_id_column.alias("seriesId"),
                sync_rid_column.alias("syncRid"),
            )
        ),
    ).otherwise(F.lit(None))
