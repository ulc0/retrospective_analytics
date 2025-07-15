from palantir_models.models import ModelAdapter, PythonEnvironment, CondaDependency, ModelStateReader
from palantir_models.models.api import ModelApi, ModelApiColumn, ModelInput, ModelOutput, DFType
from palantir_models.models._types import CondaVersionExact
import pickle
import os


class BERTopicModelAdapter(ModelAdapter):
    MODEL_SAVE_LOCATION = 'BERT_model.pkl'

    METADATA_SAVE_LOCATION = 'BERT_metadata.pkl'
    PREDICTION_COLUMN_NAME_KEY = 'PREDICTION'

    model = None
    prediction_column_name = None

    def __init__(self, model, prediction_column_name = None):
        # TODO: Fill in the model constructor.
        self.model = model
        self.prediction_column_name = prediction_column_name

    @classmethod
    def load(cls, state_reader, container_context):
        with state_reader.extract_to_temp_dir() as tmp_dir:
            model = pickle.load(open(os.path.join(tmp_dir, BERTopicModelAdapter.MODEL_SAVE_LOCATION), "rb"))
            metadata = pickle.load(open(os.path.join(tmp_dir, BERTopicModelAdapter.METADATA_SAVE_LOCATION), "rb"))
            prediction_column_name = metadata[BERTopicModelAdapter.PREDICTION_COLUMN_NAME_KEY]
        return cls(model, prediction_column_name)

    def save(self, state_writer):
        with state_writer.open(BERTopicModelAdapter.MODEL_SAVE_LOCATION, "wb") as model_file:
            pickle.dump(self.model, model_file)
        with state_writer.open(BERTopicModelAdapter.METADATA_SAVE_LOCATION, "wb") as metadata_file:
            metadata = {
                BERTopicModelAdapter.PREDICTION_COLUMN_NAME_KEY: self.prediction_column_name
            }
            pickle.dump(metadata, metadata_file)

    @classmethod
    def api(cls):
        # TODO: Edit this method to define the model API.
        inputs = [
            ModelInput.Tabular(name="input_df",
                               df_type=DFType.PANDAS,
                               columns=[ModelApiColumn(name="text_clean", type=str)])
        ]
        outputs = [
            ModelOutput.Tabular(name="output_df",
                                columns=[
                                    ModelApiColumn(name="text_clean", type=str, required=False),
                                    ModelApiColumn(name="PREDICTION", type=str)])
        ]
        return ModelApi(inputs, outputs)

    def run_inference(self, inputs, outputs):
        docs = inputs.text_clean.to_list()
        topics, probabilities = self.model.transform(docs)
        df = self.model.get_document_info(docs)
        return df

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
