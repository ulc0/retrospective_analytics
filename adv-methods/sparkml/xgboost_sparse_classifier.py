# Databricks notebook source
# MAGIC %pip install shap

# COMMAND ----------

# spark configuration
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", True)
spark.conf.set("spark.databricks.queryWatchdog.enabled", True)
spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)

# COMMAND ----------

import os
os.environ["PYSPARK_PIN_THREAD"]='false'

# COMMAND ----------

#Oscar: commented ML flow because it throwing error in the workflow

#import mlflow
#experiments_id = 201992957261982
#mlflow.set_experiment(experiment_id=experiments_id)

#print(mlflow.__version__)
#mlflow.xgboost.autolog()
#mlflow.spark.autolog()
#run=mlflow.start_run(nested=True)
#

# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------

strxgboost = f"edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox"

dbutils.widgets.text(
    "xgboostTable", defaultValue=strxgboost
)

dbutils.widgets.text("indicator","label")
dbutils.widgets.text("randomSeed","42")
dbutils.widgets.text("percentColumns","10")
xgboostTable = dbutils.widgets.get("xgboostTable")
xgboostTable = f"{xgboostTable}_{suffix}_xgboost"

print(xgboostTable)

indicator=dbutils.widgets.get("indicator")
randomSeed=int(dbutils.widgets.get("randomSeed"))
percent_columns = int(dbutils.widgets.get("percentColumns"))/100




# COMMAND ----------

db = spark.table(f"{xgboostTable}")
db.printSchema()

# COMMAND ----------

db.display()

# COMMAND ----------

"""
from pyspark.sql.functions import col, sum as sum_

# Assuming 'db' is your DataFrame
zero_columns = []

for column in db.columns:
    # Calculate the sum of each column
    total = db.select(sum_(col(column))).collect()[0][0]
    # If the sum is zero, add the column name to the list
    if total == 0:
        zero_columns.append(column)

print("Columns with all zeros:", zero_columns)
"""

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
import pyspark.sql.functions as F

random.seed(randomSeed)

# Assuming df is your DataFrame in PySpark
# Get the list of column names
columns = db.columns

# Define the percentage of columns to select
# parameter percent_columns

# Calculate the number of columns to select
num_columns_to_select = int(len(columns) * percent_columns)

# Randomly select column indices
random_columns_indices = random.sample(range(len(columns)), num_columns_to_select)

# Extract column names corresponding to the randomly selected indices
random_columns = [columns[i] for i in random_columns_indices]

# Select the randomly chosen columns
random_df = db.select(F.col(indicator), *random_columns)

db = random_df


# COMMAND ----------

"""# Assuming 'indicator' and 'randomSeed' are defined elsewhere in the notebook
assembler = VectorAssembler(
    inputCols=[col for col in db.columns if col not in ['word', indicator]], outputCol="features_xgb"
)

db_transformed = Pipeline(stages=[assembler]).fit(db).transform(db)

model_db = db_transformed.select(["features_xgb", indicator])

train, test = model_db.randomSplit([0.7, 0.3], seed=randomSeed)"""


# COMMAND ----------

"""
display(train)
display(test)
"""
db.columns[2:]

# COMMAND ----------


from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.ml.linalg import VectorUDT, DenseVector

# Assuming 'indicator' and 'randomSeed' are defined elsewhere in the notebook
assembler = VectorAssembler(
    inputCols=db.columns[2:], outputCol="features_xgb"
)  #

db_transformed = Pipeline(stages = [assembler]).fit(db).transform(db)

# Assuming `data` is your input DataFrame with a vector column named "features"
# Convert the vector column to a dense vector representation


# Define a UDF to convert sparse vectors to dense
to_dense = udf(lambda v: DenseVector(v.toArray()), VectorUDT())

# Apply the UDF to convert the 'features_xgb' column to a dense vector column 'dense_features'
db_transformed = db_transformed.withColumn("dense_features", to_dense("features_xgb"))

# Create a PCA instance with the desired number of components
pca = PCA(k=2, inputCol="dense_features", outputCol="pca_features")

# Fit the PCA model to the data
model = pca.fit(db_transformed)

# Apply the PCA model to transform the data
db_transformed = model.transform(db_transformed)

# The transformed_data DataFrame will have a new column "pca_features" containing the PCA results
#db = transformed_data

# COMMAND ----------




model_db = db_transformed.select(["features_xgb", indicator])

train, test = model_db.randomSplit([0.7, 0.3], seed=randomSeed)


xgb_model = SparkXGBClassifier(    
    num_workers=1,
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

# Convert SparkXGBoost model to XGBoost format
# Create DMatrix
dtest = xgb.DMatrix(X_test, label=y_test)

# Use the booster from SparkXGBoost
booster = best_model.get_booster()

# Use SHAP to explain the model
explainer = shap.Explainer(booster, X_test)
shap_values = explainer(X_test)

# Plot summary
shap.summary_plot(shap_values, X_test, feature_names=db.columns[2:])



# COMMAND ----------

feature_importances = booster.get_score(importance_type='weight')
feature_importances

# COMMAND ----------

## looking for issues in the autiong implementation


# Convert to DataFrame for plotting
importance_df = pd.DataFrame.from_dict(feature_importances, orient='index', columns=['importance'])

# Plot feature importance
importance_df.sort_values(by='importance', ascending=False).plot(kind='bar')
plt.title('Feature Importance')
plt.tight_layout()


# COMMAND ----------

# Plot a single prediction
shap.initjs()
shap.force_plot(shap_values[0], feature_names=db.columns[2:], matplotlib = True)

# COMMAND ----------

# Plot a single prediction
shap.force_plot(shap_values[1], feature_names=db.columns[2:])

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[2:], plot_type='bar')

# COMMAND ----------

shap.summary_plot(shap_values, feature_names=db.columns[2:])

# COMMAND ----------

shap.summary_plot(shap_values, plot_type='violin', feature_names=db.columns[2:])

# COMMAND ----------

X_test

# COMMAND ----------

y_test

# COMMAND ----------

X_test.shape    

# COMMAND ----------

y_test.shape

# COMMAND ----------

type(X_test)

# COMMAND ----------

type(y_test)

# COMMAND ----------

import numpy as np
import pandas as pd

# Check if X_test and y_test are in the right format
if isinstance(X_test, (pd.DataFrame, np.ndarray)) and isinstance(y_test, (pd.Series, np.ndarray)):
    print("Data format is correct.")
else:
    raise ValueError("X_test or y_test is not in the correct format.")


# COMMAND ----------

import shap

# Assuming shap_values is computed previously
if isinstance(shap_values, shap.Explanation):
    print("SHAP values are in the correct format.")
else:
    raise ValueError("SHAP values are not in the correct format.")


# COMMAND ----------

"""
import shap

try:
    clustering = shap.utils.hclust(X_test, y_test)
    print("Clustering completed successfully.")
except Exception as e:
    print(f"Error during clustering: {e}")
"""


# COMMAND ----------

"""
import shap

try:
    shap.plots.bar(shap_values, clustering=clustering, clustering_cutoff=0.5)
except Exception as e:
    print(f"Error during plotting: {e}")
"""


# COMMAND ----------

"""
# TODO: ask Kate to enable this part 

clustering = shap.utils.hclust(X_test, y_test)
shap.plots.bar(shap_values, clustering = clustering, clustering_cutoff=0.5)
"""


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
