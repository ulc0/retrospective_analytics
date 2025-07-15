# Databricks notebook source
import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

# spark configuration
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", True)
spark.conf.set("spark.databricks.queryWatchdog.enabled", True)
spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)

# COMMAND ----------

import os
os.environ["PYSPARK_PIN_THREAD"]='false'

# COMMAND ----------

import mlflow
experiments_id = 201992957261982
mlflow.set_experiment(experiment_id=experiments_id)

print(mlflow.__version__)
mlflow.xgboost.autolog()
mlflow.spark.autolog()
run=mlflow.start_run(nested=True)
#

# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

rfTablestring = f"edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox"

dbutils.widgets.text(
    "rfTable", defaultValue=rfTablestring
)


dbutils.widgets.text("indicator","label")
dbutils.widgets.text("randomSeed","42")
dbutils.widgets.text("percentColumns","100")

rfTable = dbutils.widgets.get("rfTable")
rfTable = f"{rfTable}_{suffix}_xgboost" # this xg boost only in name, need to change the suffix to rf



print(rfTable)
indicator=dbutils.widgets.get("indicator")
randomSeed=int(dbutils.widgets.get("randomSeed"))
percent_columns = int(dbutils.widgets.get("percentColumns"))/100
print(percent_columns)

# COMMAND ----------

db = spark.table(f"{rfTable}")
db.printSchema()

# failed: edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox_biobert_diseases_ner_xgboost
#         edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox_biobert_diseases_ner_xgboost

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier  # XGBoostEstimator,
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from xgboost.spark import SparkXGBClassifier

# COMMAND ----------

# MAGIC %md #Random forest implementation

# COMMAND ----------

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator 
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()

# COMMAND ----------

display(db)

# COMMAND ----------

# adding this piece of code for small test purposes, 10% of the features

from pyspark.sql import SparkSession
import random

random.seed(42)

# Assuming df is your DataFrame in PySpark
# Get the list of column names
columns = db.columns

# Define the percentage of columns to select
#percent_columns = 0.1

# Calculate the number of columns to select
num_columns_to_select = int(len(columns) * percent_columns)

# Randomly select column indices
random_columns_indices = random.sample(range(len(columns)), num_columns_to_select)

# Extract column names corresponding to the randomly selected indices
random_columns = sorted(
    [columns[i] for i in random_columns_indices if columns[i] not in ["label", "person_id","nosignsymtomidentified"]] # droping no signsymtomidentified column, to see how the other ones can still yield a good prediction
)

# Select the randomly chosen columns
random_df = db.select(F.col("label"),*random_columns)

db = random_df

# COMMAND ----------

display(db)

# COMMAND ----------

db.columns[1:]

# COMMAND ----------

assembler = VectorAssembler(
    inputCols=db.columns[1:], 
    outputCol="features_rf",
    handleInvalid="skip"  # or "keep" depending on your preference
)  #

db_transformed = Pipeline(stages = [assembler]).fit(db).transform(db)

model_db = db_transformed.select(["features_rf", indicator])

train, test = model_db.randomSplit([0.7, 0.3], seed=randomSeed)

rf_model = RandomForestClassifier(
    labelCol = "label", 
    numTrees = 30, 
    featuresCol="features_rf", 
    maxBins=30
)

#Define the hyperparameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_model.numTrees, [10, 20, 30])
    .addGrid(rf_model.maxDepth, [5, 10, 15])
    .build()
)
#paramGrid
evaluator = BinaryClassificationEvaluator(labelCol=indicator , metricName="areaUnderROC")
cv = CrossValidator(
    estimator=rf_model, 
    estimatorParamMaps=paramGrid, 
    evaluator=evaluator, 
    numFolds=10, # bumping folds to 10
    )
 
# Run cross validations. This step takes a few minutes and returns the best model found from the cross validation.
pipeline = Pipeline( stages = [cv])


pipelineModel = pipeline.fit(train)

#best_model = pipelineModel.stages[-1]
best_model = pipelineModel.stages[-1].bestModel

#test_transformed = assembler.transform(test)
predictions = best_model.transform(test)

display(predictions)



# COMMAND ----------

#display(
#    spark
#    .table("edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_xgboost_data")
#)

# COMMAND ----------

roc_auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
print(f"Area Under ROC: {roc_auc}")

# COMMAND ----------

sorted_features = sorted(zip(db.columns[1:], best_model.featureImportances), key=lambda x: x[1], reverse=True)

sorted_features

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.ml.functions import vector_to_array

evaluator.setMetricName("areaUnderROC")
roc_data = predictions.select("probability", indicator)

# Convert ROC data to a format suitable for plotting
roc_data = roc_data.withColumn("label", roc_data[indicator])
roc_data = roc_data.withColumn("probability", vector_to_array("probability")[1])

# Collect the ROC data to the driver
roc_data_collected = roc_data.toPandas()

# Calculate ROC Curve
from sklearn.metrics import roc_curve

fpr, tpr, thresholds = roc_curve(roc_data_collected["label"], roc_data_collected["probability"])

# Plot ROC Curve
plt.figure(figsize=(10, 6))
plt.plot(fpr, tpr, color='blue', label='ROC Curve')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve Random Forest')
plt.legend(loc='lower right')
plt.grid(True)
plt.show()



# COMMAND ----------

#### feature importance random forest

# COMMAND ----------


import numpy as np
import pandas as pd
import shap
from sklearn.ensemble import RandomForestClassifier

# Extract features and labels from Spark DataFrame
test_features = test.select("features_rf").rdd.map(lambda row: row[0].toArray()).collect()
test_labels = test.select(indicator).rdd.map(lambda row: row[0]).collect()

X_test = np.array(test_features)
y_test = np.array(test_labels)

explainer = shap.TreeExplainer(best_model) 
shap_values = explainer.shap_values(X_test)

# Plot summary
shap.summary_plot(shap_values, X_test, feature_names=db.columns[2:])

# COMMAND ----------

# Plot a single prediction
#shap.initjs()
#shap.plots.force(explainer.expected_value, shap_values[0], feature_names=db.columns[2:], matplotlib=True)

# COMMAND ----------

# Plot a single prediction
#shap.force_plot(shap_values[1], feature_names=db.columns[2:])

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[1:], plot_type='bar')

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[1:])

# COMMAND ----------

shap.summary_plot(shap_values, plot_type='violin', feature_names=db.columns[1:])
