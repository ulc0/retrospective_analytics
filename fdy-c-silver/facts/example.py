from transforms.api import transform, Input, Output, TransformContext
from transforms.verbs.dataframes import sanitize_schema_for_parquet

@transform(
    output=Output("/DCIPHER/Training Playground/cdh_silver_synthea10k/fact_person"),
    raw=Input("ri.compass.main.folder.baf3e6dd-0874-4853-b60e-a10df134fdfd"),
)
def read_csv(ctx: TransformContext, raw:Input, output:Output):
    filesystem = raw.filesystem()
    hadoop_path = filesystem.hadoop_path
    files = [f"{hadoop_path}/{f.path}" for f in filesystem.ls()]
    df = (
        ctx
        .spark_session
        .read
        .option("encoding", "UTF-8")  # UTF-8 is the default
        .option("header", True)
        .option("inferSchema", True)
        .csv(files)
    )
    output.write_dataframe(sanitize_schema_for_parquet(df))