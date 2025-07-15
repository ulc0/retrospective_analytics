# Databricks notebook source
#https://mlflow.org/docs/latest/llms/transformers/guide/index.html#id7
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
with mlflow.start_run():
    model_info = mlflow.transformers.log_model(
        transformers_model=conversational_pipeline,
        artifact_path="chatbot",
        task="conversational",
        signature=signature,
        input_example="A clever and witty question",
    )

# Load the saved pipeline as pyfunc
chatbot = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)

# Ask the chatbot a question
response = chatbot.predict("What is machine learning?")

print(response)

# >> [It's a new thing that's been around for a while.]

