# Databricks notebook source
# MAGIC %md
# MAGIC # Requirements
# MAGIC To use Spark NLP, create or use a cluster with any [compatible runtime version](https://nlp.johnsnowlabs.com/docs/en/install#databricks-support). Install Spark NLP on the cluster using Maven coordinates, such as `com.johnsnowlabs.nlp:spark-nlp_2.12:4.1.0`. 

# COMMAND ----------

# MAGIC %pip install sparknlp

# COMMAND ----------

# MAGIC %md
# MAGIC # Load sample training and evaluation data

# COMMAND ----------

# MAGIC %md
# MAGIC wget -q https://raw.githubusercontent.com/JohnSnowLabs/spark-nlp/master/src/test/resources/conll2003/eng.train
# MAGIC wget -q https://raw.githubusercontent.com/JohnSnowLabs/spark-nlp/master/src/test/resources/conll2003/eng.testa

# COMMAND ----------

# MAGIC %md
# MAGIC /Workspace/Repos/ulc0@cdc.gov/unstructured-featurization/eng.testa
# MAGIC eng.testa

# COMMAND ----------

from sparknlp.training import CoNLL
training_data = CoNLL().readDataset(spark, 'eng.train')
test_data = CoNLL().readDataset(spark, 'eng.testa')

# COMMAND ----------

# MAGIC %md
# MAGIC # Fit a pipeline on the training data

# COMMAND ----------

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

import mlflow
mlflow_run = mlflow.start_run()

max_epochs=1
lr=0.003
batch_size=32
random_seed=0
verbose=1
validation_split= 0.2
evaluation_log_extended= True
enable_output_logs= True
include_confidence= True
output_logs_path="dbfs:/ner_logs"

dbutils.fs.mkdirs(output_logs_path)

nerTagger = NerDLApproach()\
  .setInputCols(["sentence", "token", "embeddings"])\
  .setLabelColumn("label")\
  .setOutputCol("ner")\
  .setMaxEpochs(max_epochs)\
  .setLr(lr)\
  .setBatchSize(batch_size)\
  .setRandomSeed(random_seed)\
  .setVerbose(verbose)\
  .setValidationSplit(validation_split)\
  .setEvaluationLogExtended(evaluation_log_extended)\
  .setEnableOutputLogs(enable_output_logs)\
  .setIncludeConfidence(include_confidence)\
  .setOutputLogsPath(output_logs_path)

# Log model training parameters to MLflow.
mlflow.log_params({
  "max_epochs": max_epochs,
  "lr": lr,
  "batch_size": batch_size,
  "random_seed": random_seed,
  "verbose": verbose,
  "validation_split": validation_split,
  "evaluation_log_extended": evaluation_log_extended,
  "enable_output_logs": enable_output_logs,
  "include_confidence": include_confidence,
  "output_logs_path": output_logs_path
})

# The training and evaluation data is already tokenized, so you can directly 
# apply the embedding model and then fit a named-entity recognizer on the embeddings.
glove_embeddings = WordEmbeddingsModel.pretrained('glove_100d')\
          .setInputCols(["document", "token"])\
          .setOutputCol("embeddings")

ner_pipeline = Pipeline(stages=[
          glove_embeddings,
          nerTagger
 ])

ner_model = ner_pipeline.fit(training_data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Evaluate on test data

# COMMAND ----------

predictions = ner_model.transform(test_data)

# COMMAND ----------

# DBTITLE 1,Tokens, ground truth, and predictions
import pyspark.sql.functions as F
display(predictions.select(F.col('token.result').alias("tokens"),
                           F.col('label.result').alias("ground_truth"),
                           F.col('ner.result').alias("predictions")).limit(3))

# COMMAND ----------

# Reformat data to one token per row for evaluation.
predictions_pandas = predictions.select(F.explode(F.arrays_zip(predictions.token.result,
                                                     predictions.label.result,
                                                     predictions.ner.result)).alias("cols")) \
                              .select(F.expr("cols['0']").alias("token"),
                                      F.expr("cols['1']").alias("ground_truth"),
                                      F.expr("cols['2']").alias("prediction")).toPandas()

# COMMAND ----------

# DBTITLE 1,First twenty tokens and labels
display(predictions_pandas.head(20))

# COMMAND ----------

from sklearn.metrics import classification_report

# Generate a classification report.
report = classification_report(predictions_pandas['ground_truth'], predictions_pandas['prediction'], output_dict=True)

# Directly log accuracy to MLflow.
mlflow.log_metric("accuracy", report["accuracy"])
# Log the full classification report by token type as an artifact to MLflow.
mlflow.log_dict(report, "classification_report.yaml")

# Print out the report to view it in the notebook.
print (classification_report(predictions_pandas['ground_truth'], predictions_pandas['prediction']))


# COMMAND ----------

# MAGIC %md
# MAGIC # Construct and log a prediction pipeline for text

# COMMAND ----------

document = DocumentAssembler()\
    .setInputCol("text")\
    .setOutputCol("document")

sentence = SentenceDetector()\
    .setInputCols(['document'])\
    .setOutputCol('sentence')

token = Tokenizer()\
    .setInputCols(['sentence'])\
    .setOutputCol('token')

# Pull out the model from the pipeline.
loaded_ner_model = ner_model.stages[1]

converter = NerConverter()\
      .setInputCols(["document", "token", "ner"])\
      .setOutputCol("ner_span")

ner_prediction_pipeline = Pipeline(
    stages = [
        document,
        sentence,
        token,
        glove_embeddings,
        loaded_ner_model,
        converter])

# COMMAND ----------

# Fitting with an empty data frame allows you to construct a pipeline model 
# without retraining the model.
empty_data = spark.createDataFrame([['']]).toDF("text")
prediction_model = ner_prediction_pipeline.fit(empty_data)

# COMMAND ----------

# In Databricks Runtime 11.2 and 11.2 ML, model logging is handled using Databricks MLflow utilities. 
# The Databricks MLflow utilities for DBFS in Databricks Runtime 11.2 do not support all filesystem calls that 
# Spark NLP uses for model serialization. The following command disables the use of the MLflow utilities and uses
# standard DBFS support. 
import os
if os.environ["DATABRICKS_RUNTIME_VERSION"] == "11.2":
  os.environ["DISABLE_MLFLOWDBFS"] = "True"

# COMMAND ----------

# Log the model in MLflow and build a reference to the model URI.
model_name = "NerPipelineModel"
mlflow.spark.log_model(prediction_model, model_name)
mlflow.end_run()
mlflow_model_uri = "runs:/{}/{}".format(mlflow_run.info.run_id, model_name)
display(mlflow_model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Use model on text

# COMMAND ----------

# Create sample text.
text = "From the corner of the divan of Persian saddle-bags on which he was lying, smoking, as was his custom, innumerable cigarettes, Lord Henry Wotton could just catch the gleam of the honey-sweet and honey-coloured blossoms of a laburnum, whose tremulous branches seemed hardly able to bear the burden of a beauty so flamelike as theirs; and now and then the fantastic shadows of birds in flight flitted across the long tussore-silk curtains that were stretched in front of the huge window, producing a kind of momentary Japanese effect, and making him think of those pallid, jade-faced painters of Tokyo who, through the medium of an art that is necessarily immobile, seek to convey the sense of swiftness and motion. The sullen murmur of the bees shouldering their way through the long unmown grass, or circling with monotonous insistence round the dusty gilt horns of the straggling woodbine, seemed to make the stillness more oppressive. The dim roar of London was like the bourdon note of a distant organ."
sample_data = spark.createDataFrame([[text]]).toDF("text")

# Load and use the model.
mlflow_model = mlflow.spark.load_model(mlflow_model_uri)
predictions = mlflow_model.transform(sample_data)

# COMMAND ----------

# DBTITLE 1,Raw predictions
display(predictions)

# COMMAND ----------

# DBTITLE 1,Extracted entities
display(predictions.select(F.explode(F.arrays_zip(predictions.ner_span.result,predictions.ner_span.metadata)).alias("entities")) 
      .select(F.expr("entities['0']").alias("chunk"),
              F.expr("entities['1'].entity").alias("entity")))
