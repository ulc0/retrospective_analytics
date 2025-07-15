from pyspark.sql import functions as F


def pivot_json(dataframe, data_column, schema, keep_cols=None):
    """
    Maps JSON data into :class:`DataFrame` columns.

    Args:
        dataframe: Input :class:`DataFrame` with JSON column.
        data_column: Name or :class:`Column` object to parse from.
        schema: Schema to use when parsing the JSON data.
        keep_cols: Additional existing columns to retain in the output.

    Returns:
        A :class:`DataFrame` where each field in the JSON data has been converted to a column.
    """

    select_cols = ["__parsed__.*"]
    if keep_cols:
        if isinstance(keep_cols, list):
            select_cols.extend(keep_cols)
        else:
            select_cols.append(keep_cols)

    # Use a more lenient timestamp format for JSON files to allow for flexibility in the number
    # of digits in the fractional seconds part of the timestamp. This is necessary because the
    # default behavior if there are more than 3 digits in the fractional seconds part of the
    # timestamp is to silently return incorrect data. See SPARK-31758.
    # The default format is yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
    result = dataframe.withColumn(
        "__parsed__",
        F.from_json(
            data_column,
            schema,
            {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS][XXX]"},
        ),
    )

    return result.select(select_cols)
