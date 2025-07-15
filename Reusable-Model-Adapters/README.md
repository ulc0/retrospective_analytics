# Foundry Modeling
This template provides the structure for training a model in Foundry.
To train a model in Foundry, you will need to implement two components:
1. Logic to train a **model** in Foundry
2. A **ModelAdapter** - this is the logic that describes how Foundry can interact with the **model** for serialization and inference

A model trained in Foundry can be saved natively as a resource that can be used in:
- the Modeling Objectives application for evaluation and deployment to a REST API or batch deployment environment
- downstream inference builds
- further training or transfer learning to a different application
***
### Saving a trained model in Foundry
Models can be saved to `ModelOutput` with the `publish` method.
```python
from transforms.api import transform, Input
from palantir_models.transforms import ModelOutput
from model_adapter.example_adapter import ExampleModelAdapter           # This is the ModelAdapter implemented in this repository
@transform(
    training_data_input=Input("/path/to/training_data"),
    model_output=ModelOutput("/path/to/model")
)
def compute(training_data_input, model_output):
    trained_model = train_model(training_data_input)                    # 1. Train the model in a python transform
                                                                        #    You implement #train_model with your custom training logic
    wrapped_model = ExampleModelAdapter(trained_model)                  # 2. Wrap the trained model in a ModelAdapter
    model_output.publish(                                               # 3. Save the wrapped model to Foundry
        model_adapter=wrapped_model                                     #    Foundry will call ModelAdapter.save
    )
```
***
### Running Inference in Foundry
To use a model in production, it is recommended to submit the model to
the Modeling Objectives application for evaluation, review and hosting.
Alternatively, you can use the model in a python transform as below.
In this example, the ModelAdapter takes one tabular input named `input_df` and produces
one tabular output named `output_df`.
```python
from transforms.api import transform, Input, Output
from palantir_models.transforms import ModelInput
@transform(
    inference_input=Input("/path/to/inference_input"),
    model_input=ModelInput("/path/to/model"),                           # model will be an instance of ExampleModelAdapter
    inference_output=Output("/path/to/inference_output"),
)
def compute(inference_input, model_input, inference_output):             
    inference_results = model_input.transform(inference_input)          # 1. Call ModelAdapter.transform with the inputs specified in ModelAdapter.api
                                                                        # Inference results will be the returned as a named tuple of outputs from ModelAdapter.run_inference
    inference_output.write_pandas(inference_results.output_df)          # 2. Collect the desired output from the named tuple of inference result outputs and 
                                                                        # write model inference results back to Foundry
```
***
### ModelAdapter Implementation
In the `adapter.py` file, you can implement the logic for a custom model adapter. The custom model adapter can be directly consumed in this repository.
Full [documentation](/docs/foundry/model-integration/tutorial-train-code-repositories/) and [API defintion](/docs/foundry/integrate-models/model-adapter-reference/) are available in the documentation.
```python
from palantir_models.models import ModelAdapter, PythonEnvironment, CondaDependency
from palantir_models.models.api import ModelApi
class ExampleModelAdapter(ModelAdapter):
    def __init__(self, model):
        # TODO: Fill in the model constructor.
        self.model = model
    @classmethod
    def load(cls, state_reader, container_context):
        # TODO: Edit this method to deserialize the model from a filesystem.
        pass
    def save(self, state_writer):
        # TODO: Edit this method to serialize the model to a filesystem.
        pass
    @classmethod
    def api(cls):
        # TODO: Edit this method to define the model API.
        return ModelApi(inputs=[], outputs=[])
    def run_inference(self, inputs, outputs):
        # TODO: Edit this method to define how model inference works.
        pass
```
