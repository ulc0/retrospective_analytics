"""
    This is an example implementation of model inference in Foundry.

    The model is loaded into the compute function as an instance of the model adapter class.
"""

"""
from transforms.api import transform, Input, Output
from palantir_models.transforms import ModelInput


@transform(
    testing_data_input=Input("/DCIPHER/Ecosystem/data/TEST_DATASET"),
    model_input=ModelInput("/DCIPHER/Ecosystem/models/MODEL_NAME"),
    output=Output("/DCIPHER/Ecosystem/data/INFERENCE_DATASET"),
)
def compute(testing_data_input, model_input, output):
    inference_outputs = model_input.transform(testing_data_input)
    # This assumes your model adapter returns a dataframe "output_df"
    output.write_pandas(inference_outputs.output_df)
"""
