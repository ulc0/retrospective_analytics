from transforms.api import transform, Input, Output, configure
from palantir_models.transforms import ModelInput
from main.model_training.sklearn_feature_engineering import feature_engineering


@configure(profile="DRIVER_MEMORY_MEDIUM")
@transform(
    testing_data_input=Input("ri.foundry.main.dataset.94e08aed-b25e-4e65-8e30-aa7ab1151d46"),
    model_input=ModelInput("ri.models.main.model.472e9820-879b-44c3-bb52-8971d1ce56f2"),
    output=Output("ri.foundry.main.dataset.d877c671-bf6d-4f46-8922-efd61ede2a52"),
)
def compute(testing_data_input, model_input, output):
    testing_df = feature_engineering(testing_data_input.pandas())
    inference_outputs = model_input.transform(testing_df)
    # This assumes your model adapter returns a dataframe "df_out"
    output.write_pandas(inference_outputs.df_out)
