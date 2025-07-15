from transforms.api import transform  # , TransformContext, Output, TransformOutput
import transforms as T


@transform(
    dataout=T.api.Output(
        "/1CDP-Training-736bf7/1CDP Training Playground/cdh_sandbox/data/output/analysis/testoutput"
    )
)
def compute(ctx: T.api.TransformContext, dataout: T.api.TransformOutput):
    df_custom = ctx.spark_session.createDataFrame(
        [["Hello"], ["World"]], schema=["phrase"]
    )
    # Remove comment to write contents of df_custom to dataout on build
    dataout.write_dataframe(df_custom)
