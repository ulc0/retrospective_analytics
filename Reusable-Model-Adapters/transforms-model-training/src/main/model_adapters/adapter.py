"""
from palantir_models.models import ModelAdapter, PythonEnvironment, CondaDependency
from palantir_models.models.api import ModelApi, ModelApiColumn, ModelInput, ModelOutput, DFType
from palantir_models.models._types import CondaVersionExact


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
        inputs = [
            ModelInput.Tabular(name="input_df",
                               df_type=DFType.PANDAS,
                               columns=[ModelApiColumn(name="input_column", type=str)])
        ]
        outputs = [
            ModelOutput.Tabular(name="output_df",
                                columns=[ModelApiColumn(name="output_column", type=str)])
        ]
        return ModelApi(inputs, outputs)

    def run_inference(self, inputs, outputs):
        # TODO: Edit this method to define how model inference works.
        pass

    @classmethod
    def dependencies(cls):
        # DO NOT MODIFY THIS FUNCTION DEFINITION.
        # Copy this code into all model adapters published from this repo.
        # Dependencies should be added to /transforms-model-training/conda_recipe/meta.yaml
        from main._version import __version__ as generated_version_tag
        return PythonEnvironment(
            conda_dependencies=[
                CondaDependency(
                     "transforms-model-training-ri.stemma.main.repository.4b7b2703-c8e1-46e9-b3fd-9ca87386a748",
                     CondaVersionExact(version=f"{generated_version_tag}"),
                     "ri.stemma.main.repository.4b7b2703-c8e1-46e9-b3fd-9ca87386a748")
            ]
        )
"""
