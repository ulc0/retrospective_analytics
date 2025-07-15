"""
    This is an example implementation of model training in Foundry.
    To save a Model to Foundry you will need to:
      1. Implement a ModelAdapter to wrap your trained model at
          transforms-model-training/src/main/model_adapters/adapter.py
      Or 2. Import a library with an existing ModelAdapter into this repository to wrap your trained model
"""

"""
from transforms.api import transform, Input
from palantir_models.transforms import ModelOutput
from palantir_models.models import ModelVersionChangeType
from main.model_adapters.adapter import ExampleModelAdapter


@transform(
    training_data_input=Input(""),
    model_output=ModelOutput(""),
)
def compute(training_data_input, model_output):
    '''
        This function has only Foundry specific functionality.
        It extracts the training data, calls train_model to train a model, and saves the trained model to Foundry.
    '''
    training_df = training_data_input.pandas()     # Load a pandas dataframe from the TransformsInput

    model = train_model(training_df)               # Train the model

    # Wrap the trained model in a ModelAdapter
    foundry_model = ExampleModelAdapter(model)     # Edit ExampleModelAdapter for your model

    # Publish and write the trained model to Foundry
    model_output.publish(
        model_adapter=foundry_model,
        change_type=ModelVersionChangeType.MINOR   # Change type is not used and will be deprecated soon
    )


def train_model(training_df):
    '''
        This function has no Foundry specific functionality and can be replaced with your training logic.
    '''
    pass
"""
