from transforms.api import transform, Input, configure
from palantir_models.transforms import ModelOutput
from palantir_models.models import ModelVersionChangeType
from main.model_adapters.scikit_learn_adapter import SklearnModelAdapter
from sklearn.ensemble import RandomForestClassifier
from main.model_training.sklearn_feature_engineering import feature_engineering


@configure(profile="DRIVER_MEMORY_LARGE")
@transform(
    training_data_input=Input("ri.foundry.main.dataset.038aff15-c5e3-4c3e-bf75-74296800cdb9"),
    model_output=ModelOutput("ri.models.main.model.472e9820-879b-44c3-bb52-8971d1ce56f2"),
)
def compute(training_data_input, model_output):
    '''
        This function has only Foundry specific functionality.
        It extracts the training data, calls train_model to train a model, and saves the trained model to Foundry.
    '''
    training_df = training_data_input.pandas()     # Load a pandas dataframe from the TransformsInput

    training_data = feature_engineering(training_df)

    training_class = training_df['CLASIFFICATION_FINAL']

    model = RandomForestClassifier(n_estimators=100)

    model.fit(training_data, training_class)

    # Wrap the trained model in a ModelAdapter
    foundry_model = SklearnModelAdapter(model)  # Edit ExampleModelAdapter for your model

    # Publish and write the trained model to Foundry
    model_output.publish(
        model_adapter=foundry_model,
        change_type=ModelVersionChangeType.MINOR   # Change type is not used and will be deprecated soon
    )
