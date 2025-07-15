# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)  
# MAGIC [Tune GPU Performance](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp) 
# MAGIC
# MAGIC [MLFlow Auto-logging of transformers](https://mlflow.org/docs/latest/python_api/mlflow.transformers.html)   

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

# MAGIC %md
# MAGIC Autologging is known to be compatible with the following package versions: 4.25.1 <= transformers <= 4.44.2. Autologging may not succeed when used with package versions outside of this range.

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='4308303557290527')
EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')
import mlflow, transformers
mlflow.__version__
transformers.__version__
#mlflow.autolog()
mlflow.transformers.autolog()
#mlflow_run_id=mlflow.start_run(experiment_id=EXPERIMENT_ID)

# COMMAND ----------

import mlflow
import transformers

# Read a pre-trained conversation pipeline from HuggingFace hub
conversational_pipeline = transformers.pipeline(model="microsoft/DialoGPT-medium")

# Define the signature
signature = mlflow.models.infer_signature(
    "Hi there, chatbot!",
    mlflow.transformers.generate_signature_output(
        conversational_pipeline, "Hi there, chatbot!"
    ),
)

# Log the pipeline
with mlflow.start_run(experiment_id=EXPERIMENT_ID):
    model_info = mlflow.transformers.log_model(
        transformers_model=conversational_pipeline,
        artifact_path="chatbot",
        task="conversational",
        signature=signature,
        input_example="A clever and witty question",
    )



# COMMAND ----------

# Load the saved pipeline as pyfunc
chatbot = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)

# Ask the chatbot a question
response = chatbot.predict("What is machine learning?")


# COMMAND ----------


print(response)

# >> [It's a new thing that's been around for a while.]

