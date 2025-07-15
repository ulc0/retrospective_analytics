from palantir_models.models.api import ModelApi, ModelApiColumn, ModelInput, ModelOutput, DFType
from palantir_models.models import ModelAdapter
import os
import tempfile
import pickle


class pysparkModelAdapter(ModelAdapter):
    """
        :display-name: Example pyspark Model Adapter
        :description: Reference example of a model adapter for a pyspark regression model
    """
    MODEL_SAVE_LOCATION = 'pyspark_model.joblib'

    METADATA_SAVE_LOCATION = 'pyspark_metadata.pkl'
    PREDICTION_COLUMN_NAME_KEY = 'PREDICTION'

    model = None
    prediction_column_name = None
    ctx = None

    def __init__(self, model, prediction_column_name="prediction"):
        self.model = model
        self.prediction_column_name = prediction_column_name
        self.model_tmp_dir = tempfile.TemporaryDirectory()

    @classmethod
    def load(cls, state_reader, container_context, external_model_context):
        """
            ModelAdapter.load allows a model adapter to deserialize files from a filesystem - the state_reader.

            In this example we are deserializing the two items serialized in ModelAdapter.save and returning an instance of the pysparkModelAdapter.
        """
        with state_reader.extract_to_temp_dir() as tmp_dir:
            model = pickle.load(open(os.path.join(tmp_dir, pysparkModelAdapter.MODEL_SAVE_LOCATION), "rb"))

            metadata = pickle.load(open(os.path.join(tmp_dir, pysparkModelAdapter.METADATA_SAVE_LOCATION), "rb"))
            prediction_column_name = metadata[pysparkModelAdapter.PREDICTION_COLUMN_NAME_KEY]

        return cls(model, prediction_column_name)

    def save(self, state_writer):
        """
            ModelAdapter.save allows a model adapter to serialize files to a filesystem - the state_writer.

            In this example we are serializing two items:
                1. The model is being serialized to MODEL_SAVE_LOCATION with pickle
                2. Example metadata is being serialized to METADATA_SAVE_LOCATION.

            This is provided as a reference, it is expected a user will adjust this for their model.
        """
        with state_writer.open(pysparkModelAdapter.MODEL_SAVE_LOCATION, "wb") as model_file:
            pickle.dump(self.model, model_file)

        with state_writer.open(pysparkModelAdapter.METADATA_SAVE_LOCATION, "wb") as metadata_file:
            metadata = {
                pysparkModelAdapter.PREDICTION_COLUMN_NAME_KEY: self.prediction_column_name
            }
            pickle.dump(metadata, metadata_file)

    def api(self):
        inputs = [
            ModelInput.Tabular(name="df_in",
                               df_type=DFType.SPARK,
                               columns=[
                                    ModelApiColumn(name="idx", type=float, required=True),
                                    ModelApiColumn(name="text_clean", type=float, required=False),
                                    ModelApiColumn(name="target_label", type=float, required=True),
                                    ModelApiColumn(name="policy_type", type=str, required=False),
                                    ModelApiColumn(name="text", type=str, required=False)])
        ]
        outputs = [
            ModelOutput.Tabular(name="df_out",
                               columns=[
                                    ModelApiColumn(name="idx", type=float, required=True),
                                    ModelApiColumn(name="text_clean", type=float, required=False),
                                    ModelApiColumn(name="policy_type", type=str, required=False),
                                    ModelApiColumn(name="text", type=str, required=False),
                                    ModelApiColumn(name="prediction", type=float)])
        ]
        return ModelApi(inputs, outputs)

    def run_inference(self, outputs):

        df_out = outputs.df_out

        predictions = self.model.predict(df_out)
        df_out[self.prediction_column_name] = predictions
        df_out.write(df_out)
