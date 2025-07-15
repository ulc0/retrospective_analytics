import json
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from ._foundry_fs_utils import extract_timestamp, proxy_udf_generator


def json_to_df(
    transform_input,
    schema=None,
    feature_processor=None,
    content_node=None,
    content_subnode=None,
    include_file_timestamp=False,
    include_file_path=False,
    glob="**/*.json",
):
    """
    Transforms a transforms.api.Input containing json files into a dataframe.
    It currently supports two levels of content.

    Example::

        @transform(
            output=Output("/datasources/parse/uk_regional_data"),
            uk_regional=Input("/datasources/raw/ventilator"))
        def my_compute_function(ctx, uk_regional, output):
            uk_regional_data = json_to_df(uk_regional, content_node="data")
            output.write_dataframe(uk_regional_data)


    Args:
        feature_processor: If needed you can pass a custom function in this parameter to extract a row directly
            from the json `features`.
        content_node: The name of the the node where the content is present at. If there are two layers use
            `content_subnode` as well.
        content_subnode: Second level content for double nested content.
        schema: the schema of the final dataset. Useful when some columns can always be None. If you are using
            metadata columns (file_path_column and row_number_column), they need to be present. By default,
            column names are inferred.
        include_file_timestamp: Include the timestamp associated to each file as a column in the dataframe
        include_file_path: Include the path associated to each file as a column in the dataframe
    """

    def process_file(file_status):
        with transform_input.filesystem().open(file_status.path) as f:
            content = json.load(f)
            if feature_processor:
                for row in feature_processor(content["features"]):
                    if include_file_timestamp:
                        content_dict = row.asDict()
                        _add_metadata_columns(content_dict, file_status)
                        row = Row(**content_dict)
                    yield row
            else:
                if content_node:
                    if content_subnode:
                        for c in content[content_node]:
                            content_dict = c[content_subnode]
                            _add_metadata_columns(c, file_status)
                            yield Row(**content_dict)
                    else:
                        for c in content[content_node]:
                            _add_metadata_columns(c, file_status)
                            yield Row(**c)
                else:
                    for c in content:
                        _add_metadata_columns(c, file_status)
                        yield Row(**c)

    def _add_metadata_columns(row_content, file_status):
        if include_file_timestamp:
            timestamp = extract_timestamp(file_status)
            row_content.update({"__extract_timestamp__": timestamp})
        if include_file_path:
            row_content.update({"__path__": file_status.path})

    files_df = transform_input.filesystem().files(glob)

    if schema:
        process_file_udf = F.udf(proxy_udf_generator(process_file), T.ArrayType(schema))
        return files_df.withColumn(
            "__parsed", F.explode(process_file_udf("path", "modified"))
        ).select("__parsed.*")
    else:
        rdds = files_df.rdd.flatMap(process_file)
        return rdds.toDF()
