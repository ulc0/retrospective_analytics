import csv
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from ._foundry_fs_utils import extract_timestamp, proxy_udf_generator


def default_open_file(transform_input, file_status, encoding, errors):
    return transform_input.filesystem().open(
        file_status.path, encoding=encoding, errors=errors
    )


def csv_to_df(
    transform_input,
    complete_header=None,
    delimiters_to_use=[","],
    encoding="utf-8",
    decode_errors="strict",
    file_partitions=None,
    file_path_column=None,
    file_row_header=None,
    glob="**/*.csv",
    has_header=True,
    open_file=default_open_file,
    row_number_column=None,
    schema=None,
    skip_jagged_rows=False,
    skip_first_rows=0,
    column_renames=None,
    contiguous_header=False,
    match_header_length=False,
    include_file_timestamp=False,
    field_size_limit=None,
):
    """
    Transforms a transforms.api.Input containing csv files into a dataframe, useful for doing manual parsing to prevent
    magritte rest from failing to update the schema.

    Example::

        @transform(
            out=Output('/datasources/parsed/be_adm1-2_cases'),
            raw=Input('/datasources/raw/COVID19BE_CASES_AGESEX'))
        def my_compute_function(raw, out):
            df = csv_to_df(raw, encoding='ISO-8859-1')
            out.write_dataframe(df)


    Args:
        complete_header: if there are multiple files that have a different number of columns, you can provide a list of
            all columns that you would expect to be present. When set to None, expects all files to contain the same
            columns. Defaults to None.
        delimiters_to_use: a list of delimiters to use to separate columns. Defaults to [',']. It is a list to account
            for cases when different delimiters are used. The actual delimiter is inferred from the list, using the list
            order as a priority.
        encoding: the encoding of the file. Defaults to 'UTF-8'.
        decode_errors: how to handle errors while decoding files (e.g. 'ignore', 'replace'). Defaults to 'strict'.
        file_partitions: the number of partitions to split the files into before parsing.
        file_path_column: name of the column where to store the file path information. When set to None, it won't be
            stored. Defaults to None.
        file_row_header: list of strings to use as the column names. Only used if the has_header option is set to
            False.
        glob: the glob to use to select the files from the TransformInput to parse
        has_header: when set to True, use the first row of the file to get the column names
        include_file_timestamp: Include the timestamp associated to each file as a column in the dataframe
        open_file: the function to use to open the file. The function gets the TransformInput object, file_status and
            the encoding. Useful when dealing with archives. Defaults to just opening the file directly.
        row_number_column: the name of the column to use to store the row numbers. When set to None, it won't be
            stored. Defaults to None.
        schema: the schema of the final dataset. Useful when some columns can always be None. If you are using
            metadata columns (file_path_column and row_number_column), they need to be present. By default, column names
            are inferred.
        skip_jagged_rows: if True, skips jagged rows. If False, raises a RuntimeError if there is a jagged row.
            Defaults to False.
        skip_first_rows: skips the first n rows. Defaults to 0
        column_renames: map of initial column names to target column names. Defaults to None.
        contiguous_header: if True, expects that the header occupies fully adjacent cells; any values beyond an empty
            cell, when reading left-to-right, will be dropped. Defaults to False.
        match_header_length: if True, truncates the row to the length of the header. Defaults to False.
        field_size_limit: if given, sets the maximum field size limit. Defaults to the Python default (131072).

    There are a few header options, that have slightly different semantics. The algorithm for choosing the header is
    the following:

        1 - Figure out the individual file's header:
            1 - If has_header is True, use the first row as the header.
                1 - If contiguous_header is True, take the leftmost values before an empty header cell is encountered.
            2 - If file_row_header is not None, use file_row_header.
            3 - Otherwise, use the columns from the schema to infer the header.
        2 - Figure out the full header:
            1 - If complete_header is present, add the metadata columns and complete_header to get the full header.
            2 - Otherwise, add the metadata columns and the file header to get the full header.
        3 - For each row:
            1 - Fill in the columns from the row using the inferred file_row_header.
                1 - If match_header_length is True, truncate any excess columns to the right
            2 - Fill in the metadata columns.
            3 - Create a new row for the output dataframe that has all of the columns from the full header.
    """
    # TODO: log jagged rows in a dataset that is monitored for # of rows. As a ref The DataFrameReader.json()
    # implementation will write invalid rows into a special column.

    def _metadata_columns():
        columns = []
        if file_path_column:
            columns.append(file_path_column)
        if row_number_column:
            columns.append(row_number_column)
        return columns

    def _row(file_path, row_number, row, row_header, header_dict):
        ret = ["" for i in range(len(header_dict))]
        if file_path_column:
            ret[header_dict[file_path_column]] = file_path
        if row_number_column:
            ret[header_dict[row_number_column]] = row_number
        for value, header in zip(row, row_header):
            ret[header_dict[header]] = value
        return ret

    def _full_header(file_header):
        if complete_header:
            return _metadata_columns() + complete_header
        return _metadata_columns() + file_header

    def _header_dict(header):
        return {key: index for index, key in enumerate(header)}

    def _row_header(reader):
        if has_header:
            header = next(reader)
            if contiguous_header:
                # Take only the left-most columns before an empty cell
                end_index = len(header) if "" not in header else header.index("")
                return header[0:end_index]
            else:
                return header
        if file_row_header:
            return file_row_header
        return [x.name for x in schema]

    def infer_delimiter(file_status):
        # if we can, we avoid parsing the file to infer the delimiter
        if len(delimiters_to_use) == 1:
            return delimiters_to_use[0]
        delimiters = "".join(delimiters_to_use)
        with open_file(transform_input, file_status, encoding, decode_errors) as f:
            dialect = csv.Sniffer().sniff(f.read(), delimiters=delimiters)
            return dialect.delimiter

    def process_file(file_status):
        if field_size_limit is not None:
            csv.field_size_limit(field_size_limit)

        delimiter = infer_delimiter(file_status)
        if include_file_timestamp:
            timestamp = extract_timestamp(file_status)
        has_timestamp = include_file_timestamp and (timestamp is not None)
        with open_file(transform_input, file_status, encoding, decode_errors) as f:
            r = csv.reader(f, delimiter=delimiter)

            for _ in range(skip_first_rows):
                next(r)

            row_header = _row_header(r)
            if has_timestamp:
                row_header.append("timestamp")

            full_header = _full_header(row_header)
            MyRow = Row(*full_header)
            header_dict = _header_dict(full_header)

            for row_count, row in enumerate(r):
                if has_timestamp:
                    row.append(timestamp)

                if len(row) != len(row_header):
                    if skip_jagged_rows:
                        continue
                    elif match_header_length:
                        row = row[0 : len(row_header)]
                    else:
                        row_length = len(row)
                        expected_length = len(row_header)
                        raise RuntimeError(
                            f"Jagged row encountered in {file_status.path}: row #{row_count} (has {row_length} "
                            + f"columns, expected {expected_length})"
                        )
                yield MyRow(
                    *_row(file_status.path, row_count, row, row_header, header_dict)
                )

    files_df = transform_input.filesystem().files(glob)
    if file_partitions:
        files_df = files_df.repartition(file_partitions)

    if schema:
        process_file_udf = F.udf(proxy_udf_generator(process_file), T.ArrayType(schema))
        df = files_df.withColumn(
            "__parsed", F.explode(process_file_udf("path", "modified"))
        ).select("__parsed.*")
    else:
        rdds = files_df.rdd.flatMap(process_file)
        df = rdds.toDF()

    if column_renames:
        for old_name, new_name in column_renames.items():
            df = df.withColumnRenamed(old_name, new_name)

    return df
