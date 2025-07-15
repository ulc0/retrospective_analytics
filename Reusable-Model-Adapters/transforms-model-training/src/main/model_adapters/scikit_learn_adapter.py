import os
import pickle
# import sklearn
# import pandas as pd


from palantir_models.models.api import ModelApi, ModelApiColumn, ModelInput, ModelOutput, DFType
from palantir_models.models import ModelAdapter


class SklearnModelAdapter(ModelAdapter):
    """
        :display-name: Example Sklearn Model Adapter
        :description: Reference example of a model adapter for a sklearn regression model
    """
    MODEL_SAVE_LOCATION = 'sklearn_model.pkl'

    METADATA_SAVE_LOCATION = 'sklearn_metadata.pkl'
    PREDICTION_COLUMN_NAME_KEY = 'PREDICTION'

    model = None
    prediction_column_name = None

    def __init__(self, model, prediction_column_name="prediction"):
        self.model = model
        self.prediction_column_name = prediction_column_name

    @classmethod
    def load(cls, state_reader, container_context, external_model_context):
        """
            ModelAdapter.load allows a model adapter to deserialize files from a filesystem - the state_reader.

            In this example we are deserializing the two items serialized in ModelAdapter.save and returning an instance of the SklearnModelAdapter.
        """
        with state_reader.extract_to_temp_dir() as tmp_dir:
            model = pickle.load(open(os.path.join(tmp_dir, SklearnModelAdapter.MODEL_SAVE_LOCATION), "rb"))

            metadata = pickle.load(open(os.path.join(tmp_dir, SklearnModelAdapter.METADATA_SAVE_LOCATION), "rb"))
            prediction_column_name = metadata[SklearnModelAdapter.PREDICTION_COLUMN_NAME_KEY]

        return cls(model, prediction_column_name)

    def save(self, state_writer):
        """
            ModelAdapter.save allows a model adapter to serialize files to a filesystem - the state_writer.

            In this example we are serializing two items:
                1. The model is being serialized to MODEL_SAVE_LOCATION with pickle
                2. Example metadata is being serialized to METADATA_SAVE_LOCATION.

            This is provided as a reference, it is expected a user will adjust this for their model.
        """
        with state_writer.open(SklearnModelAdapter.MODEL_SAVE_LOCATION, "wb") as model_file:
            pickle.dump(self.model, model_file)

        with state_writer.open(SklearnModelAdapter.METADATA_SAVE_LOCATION, "wb") as metadata_file:
            metadata = {
                SklearnModelAdapter.PREDICTION_COLUMN_NAME_KEY: self.prediction_column_name
            }
            pickle.dump(metadata, metadata_file)

    def api(self):
        inputs = [
            ModelInput.Tabular(name="df_in",
                               df_type=DFType.PANDAS,
                               columns=[
                                    ModelApiColumn(name="USMER", type=float, required=False),
                                    ModelApiColumn(name="MEDICAL_UNIT", type=float, required=False),
                                    ModelApiColumn(name="SEX", type=float, required=False),
                                    ModelApiColumn(name="PATIENT_TYPE", type=float, required=False),
                                    ModelApiColumn(name="INTUBED", type=float, required=False),
                                    ModelApiColumn(name="PNEUMONIA", type=float, required=False),
                                    ModelApiColumn(name="AGE", type=float, required=False),
                                    ModelApiColumn(name="PREGNANT", type=float, required=False),
                                    ModelApiColumn(name="DIABETES", type=float, required=False),
                                    ModelApiColumn(name="COPD", type=float, required=False),
                                    ModelApiColumn(name="ASTHMA", type=float, required=False),
                                    ModelApiColumn(name="INMSUPR", type=float, required=False),
                                    ModelApiColumn(name="HIPERTENSION", type=float, required=False),
                                    ModelApiColumn(name="OTHER_DISEASE", type=float, required=False),
                                    ModelApiColumn(name="CARDIOVASCULAR", type=float, required=False),
                                    ModelApiColumn(name="OBESITY", type=float, required=False),
                                    ModelApiColumn(name="RENAL_CHRONIC", type=float, required=False),
                                    ModelApiColumn(name="TOBACCO", type=float, required=False),
                                    ModelApiColumn(name="ICU", type=float, required=False)])
        ]
        outputs = [
            ModelOutput.Tabular(name="df_out",
                                columns=[
                                    ModelApiColumn(name="PREDICTION", type=int)])
        ]
        return ModelApi(inputs, outputs)

    def run_inference(self, inputs, outputs):
        df_in = inputs.df_in
        predictions = self.model.predict(df_in)
        df_in[self.prediction_column_name] = predictions
        outputs.df_out.write(df_in)
