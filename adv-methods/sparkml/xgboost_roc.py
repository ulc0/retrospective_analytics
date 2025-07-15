# Databricks notebook source
# MAGIC %pip install shap
# MAGIC %restart_python

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
experiments_id = 90645967493616 #201992957261982
mlflow.set_experiment(experiment_id=experiments_id)

print(mlflow.__version__)
# no autolog with spark.xgboost mlflow.xgboost.autolog()
# ---> use mlflow.spark.log_model(spark_xgb_model, artifact_path).
mlflow.spark.autolog()
run=mlflow.start_run(nested=True)
#

# COMMAND ----------

dbutils.widgets.text(
    "gboostTable", "edav_prd_cdh.cdh_abfm_phi_ra.mpox_xgboost_data_biobert_diseases_ner"
)
dbutils.widgets.text("indicator","label")
dbutils.widgets.text("randomSeed","42")
dbutils.widgets.text("percentColumns","10")


gboostTable = dbutils.widgets.get("gboostTable")
print(gboostTable)
indicator=dbutils.widgets.get("indicator")
randomSeed=int(dbutils.widgets.get("randomSeed"))
percent_columns = int(dbutils.widgets.get("percentColumns"))/100




# COMMAND ----------

gboostbl = spark.table(f"{gboostTable}")
gboostbl.printSchema()

# COMMAND ----------

#gboostbl.summary().show()

# COMMAND ----------

# MAGIC %md #XGboost implementation

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

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

# adding this piece of code for small test purposes, 10% of the features

import random

random.seed(randomSeed)

# Assuming df is your DataFrame in PySpark
# Get the list of column names
columns = gboostbl.columns
print(columns)
# Define the percentage of columns to select
# parameter percent_columns

# Calculate the number of columns to select
num_columns_to_select = int(len(columns) * percent_columns)

# Randomly select column indices
random_columns_indices = random.sample(range(len(columns)), num_columns_to_select)

# Extract column names corresponding to the randomly selected indices
random_columns = [columns[i] for i in random_columns_indices]

# Select the randomly chosen columns
random_df = gboostbl.select(F.col(indicator), *random_columns)

#db = random_df

# COMMAND ----------

"""
# Assuming 'indicator' and 'randomSeed' are defined elsewhere in the notebook
assembler = VectorAssembler(
    inputCols=[col for col in random_df.columns if col not in ['word', indicator]], outputCol="features_xgb"
)

db_transformed = Pipeline(stages=[assembler]).fit(random_df).transform(random_df)

model_db = db_transformed.select(["features_xgb", indicator])

train, test = model_db.randomSplit([0.7, 0.3], seed=randomSeed)
"""

# COMMAND ----------

"""
display(train)
display(test)
"""

# COMMAND ----------

num_works=1 #sc.defaultParallelism
print(num_works)
# Assuming 'indicator' and 'randomSeed' are defined elsewhere in the notebook
assembler = VectorAssembler(
    inputCols=random_df.columns[2:], outputCol="features_xgb"
)  #

db_transformed = Pipeline(stages = [assembler]).fit(random_df).transform(random_df)

model_db = db_transformed.select(["features_xgb", indicator])

train, test = model_db.randomSplit([0.7, 0.3], seed=randomSeed)


xgb_model = SparkXGBClassifier(    
    num_workers=num_works,
    device="gpu",
    enable_sparse_data_optim=True,
    missing=0.0,
    features_col="features_xgb",
    label_col=indicator,
    validate_parameters=True,
)

paramGrid = (
    ParamGridBuilder()
    .addGrid(xgb_model.max_depth, [5, 7, 10])
    .addGrid(xgb_model.n_estimators, [10, 100])
    .build()
)

evaluator = BinaryClassificationEvaluator(labelCol=indicator , metricName="areaUnderROC")

cv = CrossValidator(
    estimator=xgb_model,
    estimatorParamMaps=paramGrid,    
    numFolds=2,
    evaluator = evaluator
)

#pipeline = Pipeline( stages = [assembler, cv])
pipeline = Pipeline( stages = [cv])


pipelineModel = pipeline.fit(train)

#best_model = pipelineModel.stages[-1]
best_model = pipelineModel.stages[-1].bestModel

#test_transformed = assembler.transform(test)
predictions = best_model.transform(test)

display(predictions)

# COMMAND ----------

#print (best_model.getNumFeatures())

# COMMAND ----------

roc_auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
print(f"Area Under ROC: {roc_auc}")


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
plt.title('ROC Curve xGBoost')
plt.legend(loc='lower right')
plt.grid(True)
#mlflow.log_figure(plt, 'my_plot.png')
plt.show()



# COMMAND ----------

# assembling all feature vectors that are not in the indexer
# https://medium.com/@nutanbhogendrasharma/feature-transformer-vectorassembler-in-pyspark-ml-feature-part-3-b3c2c3c93ee9
#assembler = VectorAssembler(
#    inputCols=db.columns[2:], outputCol="features_xgb"
#)  # there is a column 'word' called feature


#db_transformed = Pipeline(stages=[assembler]).fit(db).transform(db)

#model_db = db_transformed.select(["features_xgb", indicator])
#TODO mlflow
#TODO parameterize

#train_data, test_data = model_db.randomSplit([0.7, 0.3], seed=randomSeed)
#TODO parameterize


# COMMAND ----------

import shap
import numpy as np
import pandas as pd
import xgboost as xgb


# COMMAND ----------

# Collect test features and labels for SHAP
test_features = test.select("features_xgb").rdd.map(lambda row: row[0].toArray()).collect()
test_labels = test.select(indicator).rdd.map(lambda row: row[0]).collect()

# Convert to numpy arrays
X_test = np.array(test_features)
y_test = np.array(test_labels)




# COMMAND ----------

# Convert SparkXGBoost model to XGBoost format
# Create DMatrix
dtest = xgb.DMatrix(X_test, label=y_test)

# Use the booster from SparkXGBoost
booster = best_model.get_booster()


# COMMAND ----------

# Use SHAP to explain the model
explainer = shap.Explainer(booster, X_test)


# COMMAND ----------



shap_values = explainer(X_test)
# Plot summary
shap.summary_plot(shap_values, X_test, feature_names=gboostbl.columns[2:])


# COMMAND ----------

# Plot a single prediction
shap.initjs()
shap.force_plot(shap_values[0], feature_names=db.columns[2:], matplotlib = True)

# COMMAND ----------

# Plot a single prediction
shap.initjs()
shap.force_plot(shap_values[1], feature_names=db.columns[2:])

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[2:], plot_type='bar')

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[2:])

# COMMAND ----------

shap.summary_plot(shap_values, plot_type='violin', feature_names=db.columns[2:])

# COMMAND ----------

print(X_test)

# COMMAND ----------

# TODO: ask Kate to enable this part 

clustering = shap.utils.hclust(X_test, y_test)
shap.plots.bar(shap_values, clustering = clustering, clustering_cutoff=0.5)



# COMMAND ----------

#shap.summary_plot(shap_values, feature_names=db.columns[2:], plot_type='bar', clustering = clustering)

# COMMAND ----------

#TODO mlflow
"""
print("all data", model_db.count())
print("train data", train_data.count())
print(f"train data {indicator}", train_data.where(f"{indicator}=1").count())
print("test data", test_data.count())
print(f"test data {indicator}", test_data.where(f"{indicator}=1").count())
"""

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------



# COMMAND ----------

# Source https://stackoverflow.com/questions/52847408/pyspark-extract-roc-curve

# Scala version implements .roc() and .pr()
# Python: https://spark.apache.org/docs/latest/api/python/_modules/pyspark/mllib/common.html
# Scala: https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/evaluation/BinaryClassificationMetrics.html

from pyspark.mllib.evaluation import BinaryClassificationMetrics


class CurveMetrics(BinaryClassificationMetrics):
    def __init__(self, *args):
        super(CurveMetrics, self).__init__(*args)

    def _to_list(self, rdd):
        points = []
        # Note this collect could be inefficient for large datasets
        # considering there may be one probability per datapoint (at most)
        # The Scala version takes a numBins parameter,
        # but it doesn't seem possible to pass this from Python to Java
        for row in rdd.collect():
            # Results are returned as type scala.Tuple2,
            # which doesn't appear to have a py4j mapping
            points += [(float(row._1()), float(row._2()))]
        return points

    def get_curve(self, method):
        rdd = getattr(self._java_model, method)().toJavaRDD()
        return self._to_list(rdd)


# COMMAND ----------

import matplotlib.pyplot as plt

# Create a Pipeline estimator and fit on train DF, predict on test DF
model = classifier
predictions = predictions

# Returns as a list (false positive rate, true positive rate)
preds = predictions.select(indicator, "probability").rdd.map(
    lambda row: (float(row["probability"][1]), float(row[indicator]))
)
points = CurveMetrics(preds).get_curve("roc")

plt.figure()
x_val = [x[0] for x in points]
y_val = [x[1] for x in points]
plt.title("ROC curve")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.plot(x_val, y_val)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType

preds_and_labels = (
    predictions.select(["prediction", indicator])
    .withColumn(indicator, F.col(indicator).cast(FloatType()))
    .orderBy("prediction")
)

preds_and_labels = preds_and_labels.select(["prediction", indicator])
metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
print(metrics.confusionMatrix().toArray())

# COMMAND ----------

display(predictions)

# COMMAND ----------

print(preds_and_labels.where("prediction = 1").count())
print(preds_and_labels.where(f"{indicator} = 1").count())
