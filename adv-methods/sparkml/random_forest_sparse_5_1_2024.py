# Databricks notebook source
import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

dbutils.widgets.text('task_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("task_name")
print(architecture)


# COMMAND ----------

dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf")
input_table_name = dbutils.widgets.get("input_table_name")
input_table_name

# COMMAND ----------

model_suffix=architecture.split('/')[-1].replace('-',"_")
print(model_suffix)

# COMMAND ----------



# COMMAND ----------


# Maximize Pandas output text width.

import pandas as pd
from datetime import date
from itertools import chain
import math
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
import numpy as np


#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row
import sparkml

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

#import pyspark sql functions with alias
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp, greatest, least, ceil, expr
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, length, col

#spark.sql("SHOW DATABASES").show(truncate=False)
spark.conf.set('spark.sql.shuffle.partitions',7200*4)


# COMMAND ----------

entities_to_select = spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf"+f"_{model_suffix}")

# COMMAND ----------

notes_stats = (
    entities_to_select
    .select("person_id","mpox_visit","note_text",) 
    .dropDuplicates() 
    .withColumn("note_lenght",
                F.length(F.col("note_text"))
                )      
)
"""
display(
    notes_stats
    .summary("count", "25%", "50%", "75%", "mean", "stddev")
#    .groupBy("mpox_exposed")
)

display(
    notes_stats
    .groupBy("note_lenght")
    .count()
    .sort("count", ascending = False)
    )

notes_stats.count()
"""

# COMMAND ----------


spark.conf.set("spark.sql.pivotMaxValues", 100000)
entities_to_select_filtered = (
    entities_to_select
    .filter(length(f"word_{model_suffix}") > 2)
    .withColumn("cleaned_text", regexp_replace(f"word_{model_suffix}", r"([^\s\w]|_)+", ""))
    .withColumn("cleaned_text", trim(regexp_replace(col("cleaned_text"), r"\s+", " ")))
    .drop(f"word_{model_suffix}")
    .withColumnRenamed("cleaned_text",f"word_{model_suffix}")
)
"""
display(
    entities_to_select_filtered
    .select(f"word_{model_suffix}","mpox_visit")
    .groupBy(f"word_{model_suffix}","mpox_visit")    
    .count()
    .sort("mpox_visit","count", ascending = False)
)
"""
data_set = (
    entities_to_select_filtered
    .select("person_id","mpox_visit",f"word_{model_suffix}")
    .groupBy("person_id","mpox_visit")
    .pivot(f"word_{model_suffix}")
    .count()
    .fillna(0)
    .withColumnRenamed("mpox_visit", "label")
    
)
"""
display(data_set)
print(entities_to_select_filtered.select(f"word_{model_suffix}").dropDuplicates().count())
print(data_set.select("person_id").dropDuplicates().count())
#display(entities_to_select_filtered)
"""

#display(data_set)

# COMMAND ----------

#cat_variable = ["statecode", "age_group", "gender"]
cat_variable = ["age_group", "gender"]

#dependent_variables = (
#    dataframe.select(*cat_variable)              
#)

indep_variable = ["label"]

db = data_set.drop("patientuid")
display(db)

# COMMAND ----------

#display(dependent_variables)
#print((dependent_variables.count(), len(dependent_variables.columns)))

#display(
#    dataframe
#    .select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns])
#)

# COMMAND ----------

len(db.columns) 

# COMMAND ----------

print("total number of patients that had a disease/ symthom identified with the ner", data_set.where("label =1").select("person_id").dropDuplicates().count())

# COMMAND ----------

db.where("label =1").count()

# COMMAND ----------

#display(db)

#from pyspark.sql.functions import col, countDistinct

#display(db.agg(*(countDistinct(col(c)).alias(c) for c in db.columns)))

# COMMAND ----------

#db.columns[4:]

# COMMAND ----------

db.columns

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
percent_columns = 0.1

# Calculate the number of columns to select
num_columns_to_select = int(len(columns) * percent_columns)

# Randomly select column indices
random_columns_indices = random.sample(range(len(columns)), num_columns_to_select)

# Extract column names corresponding to the randomly selected indices
random_columns = [columns[i] for i in random_columns_indices]

# Select the randomly chosen columns
random_df = db.select(F.col("label"),*random_columns)

db = random_df

# COMMAND ----------

# assembling all feature vectors that are not in the indexer
#https://medium.com/@nutanbhogendrasharma/feature-transformer-vectorassembler-in-pyspark-ml-feature-part-3-b3c2c3c93ee9
assembler = VectorAssembler(inputCols = db.columns[2:], outputCol = "features_rf" ) # there is a column 'word' called feature
pipeline = Pipeline(stages = [assembler])

db_transformed = pipeline.fit(db).transform(db)


# COMMAND ----------

model_db = db_transformed.select(["features_rf","label"])

# COMMAND ----------

model_db.show()

# COMMAND ----------

train_data, test_data = model_db.randomSplit([0.7, 0.3], seed=42)

# COMMAND ----------

print("all data",model_db.count())
print("train data",train_data.count())
print("train data label",train_data.where("label =1").count())
print("test data",test_data.count())
print("test data label",test_data.where("label =1").count())

# COMMAND ----------

# for classification is recommended sqr(p) where p is the number of features that is 35 in

rf_model = RandomForestClassifier(labelCol = "label", numTrees = 30, featuresCol="features_rf", maxBins=30)
pipeline_parameter_exploration = Pipeline(stages=[rf_model]) 
#pipeline_parameter_exploration

# COMMAND ----------

#Define the hyperparameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_model.numTrees, [10])
    .build()
)
#paramGrid

# COMMAND ----------

cv = CrossValidator(
    estimator=pipeline_parameter_exploration, 
    estimatorParamMaps=paramGrid, 
    evaluator=BinaryClassificationEvaluator(), 
    numFolds=2, 
    )
 
# Run cross validations. This step takes a few minutes and returns the best model found from the cross validation.
#cv

# COMMAND ----------

cvModel = cv.fit(train_data)
cvModel

# COMMAND ----------

#dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------

best_rf_model = cvModel.bestModel.stages[-1]
importances = best_rf_model.featureImportances.toArray()



# COMMAND ----------

classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 60)
paramGrid = (ParamGridBuilder()
            .addGrid(classifier.maxDepth, [20,30])
            #.addGrid(classifier.maxBins, [10,20,30]) # Iprior runs for models make seems this to be unecessary
            .addGrid(classifier.numTrees,[30, 60, 90]) # some models are better with 30 other with 60
            .build())
crossval = CrossValidator(estimator=classifier,
                         estimatorParamMaps=paramGrid,
                         evaluator=MulticlassClassificationEvaluator(),
                         numFolds=5,
                         parallelism = 7
                         )
fitModel = crossval.fit(train_data)
BestModel= fitModel.bestModel
featureImportances= BestModel.featureImportances.toArray()

# COMMAND ----------

schema = StructType([
    StructField("Classifier", StringType(), True),
    StructField("Result", DoubleType(), True),
])

# COMMAND ----------

maximum  = len(db.columns) 
n = 15 # limiting to at most 30 features

print(maximum)
print(n)

# COMMAND ----------

"""
# source https://antonhaugen.medium.com/feature-selection-for-pyspark-tree-classifiers-3be54a8bc493

results = spark.createDataFrame([],schema)
MC_evaluator = MulticlassClassificationEvaluator(metricName="f1")

crossval = (
    CrossValidator(
        estimator=classifier,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=5,
        parallelism = 7
        )
)
#classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 60, maxBins=60, maxDepth = 50)

for n in range(10, n, 10):
    print("Testing top n= ", n, " features")
    
    best_n_features= importances.argsort()[-n:][::-1]
    best_n_features= best_n_features.tolist()
    vs= VectorSlicer(inputCol='features_rf',   outputCol='best_features',indices=best_n_features)
    bestFeaturesDf= vs.transform(model_db)
    train,test= bestFeaturesDf.randomSplit([0.7, 0.3])
        
    fitModel = crossval.fit(train)

    BestModel= fitModel.bestModel
    featureImportances = BestModel.featureImportances.toArray()
    print("Feature importances: ", featureImportances)
  
    predictions = fitModel.transform(test)

    accuracy = (MC_evaluator.evaluate(predictions))*100
    new_row = spark.createDataFrame([(f"Top {n} features", accuracy)],schema)
    results = results.union(new_row)
    

display(results)
"""

# COMMAND ----------

# source https://antonhaugen.medium.com/feature-selection-for-pyspark-tree-classifiers-3be54a8bc493

results = spark.createDataFrame([],schema)
MC_evaluator = MulticlassClassificationEvaluator(metricName="f1")

crossval = (
    CrossValidator(
        estimator=classifier,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=5,
        parallelism = 7
        )
)
#classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 60, maxBins=60, maxDepth = 50)
best_feature_names = []
for n in range(10, n, 10):
    print("Testing top n= ", n, " features")
    
    best_n_features= importances.argsort()[-n:][::-1]
    best_n_features= best_n_features.tolist()
    vs= VectorSlicer(inputCol='features_rf',   outputCol='best_features',indices=best_n_features)
    bestFeaturesDf= vs.transform(model_db)

    # Retrieve feature names
    selected_feature_names = bestFeaturesDf.schema["features_rf"].metadata["ml_attr"]["attrs"]["numeric"]
    selected_feature_names = [attr["name"] for attr in selected_feature_names]

    # Select only the names of the best features
    selected_best_feature_names = [selected_feature_names[i] for i in best_n_features]
    best_feature_names.append(selected_best_feature_names)
    
    train,test= bestFeaturesDf.randomSplit([0.7, 0.3])
        
    fitModel = crossval.fit(train)

    BestModel= fitModel.bestModel
    featureImportances = BestModel.featureImportances.toArray()
    print("Feature importances: ", featureImportances)
  
    predictions = fitModel.transform(test)

    accuracy = (MC_evaluator.evaluate(predictions))*100
    new_row = spark.createDataFrame([(f"Top {n} features", accuracy)],schema)
    results = results.union(new_row)
    

print("Best feature names:", best_feature_names)

# COMMAND ----------

# updating code for storing classifier and result
#maximum  = 1204
#n = 35

#classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 30, maxBins=60)
#for n in range(10, maximum, 10):
#    print("Testing top n= ", n, " features")
    
#    best_n_features= importances.argsort()[-n:][::-1]
#    best_n_features= best_n_features.tolist()
#    vs= VectorSlicer(inputCol='features_rf',   outputCol='best_features',indices=best_n_features)
#    bestFeaturesDf= vs.transform(model_db)
#    train,test= bestFeaturesDf.randomSplit([0.7, 0.3])
    
#    columns = ['Classifier', 'Result']
#    vals = [('Place Holder', 'N/A')]
#    results = spark.createDataFrame(vals, columns)
    
#    paramGrid = (ParamGridBuilder()\
#            .addGrid(classifier.maxDepth, [2, 5, 10])
#            .addGrid(classifier.maxBins, [40, 60 , 80])
#            .addGrid(classifier.numTrees,[5, 30, 60])
#            .build())

# COMMAND ----------

display(train)
display(test)

# COMMAND ----------

print("all data",bestFeaturesDf.count())
print("train data",train.count())
print("train data label",train.where("label =1").count())
print("test data",test.count())
print("test data label",test.where("label =1").count())

# COMMAND ----------

BestModel= fitModel.bestModel
BestModel

# COMMAND ----------

featureImportances= BestModel.featureImportances.toArray()
print("Feature Importances: ", featureImportances)


# COMMAND ----------

display(featureImportances)

# COMMAND ----------

predictions = fitModel.transform(test)
print(predictions)

# COMMAND ----------

predictions

# COMMAND ----------

bcEvaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
print(f"Area under ROC curve: {bcEvaluator.evaluate(predictions)}")

# COMMAND ----------

mcEvaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print(f"Accuracy: {mcEvaluator.evaluate(predictions)}")

# COMMAND ----------

mcEvaluator_f1 = MulticlassClassificationEvaluator(metricName="f1")
print(f"f1: {mcEvaluator_f1.evaluate(predictions)}")

# COMMAND ----------

attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*predictions.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
    )
) 


# COMMAND ----------

var_imp_list = [(name, fitModel.bestModel.featureImportances[idx]) for idx, name in attrs if fitModel.bestModel.featureImportances[idx]]

# COMMAND ----------

sorted_data_list = sorted(var_imp_list, key = lambda x: x[1], reverse=True)
top_words = [item[0] for item in sorted_data_list[:round(math.sqrt(len(random_df.columns)))]] # rounding to the sqr of the initial number of features



# COMMAND ----------

sorted_data_list

# COMMAND ----------

top_words


# COMMAND ----------

print(top_words)

# COMMAND ----------

#rf_predictions = rf_classifier.transform(test_data[[top_words]])

# COMMAND ----------

#rf_predictions

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
preds = predictions.select('label','probability').rdd.map(lambda row: (float(row['probability'][1]), float(row['label'])))
points = CurveMetrics(preds).get_curve('roc')

plt.figure()
x_val = [x[0] for x in points]
y_val = [x[1] for x in points]
plt.title('ROC curve')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.plot(x_val, y_val)

# COMMAND ----------

from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

preds_and_labels = (
    predictions
    .select(['prediction','label'])
    .withColumn('label', F.col('label').cast(FloatType()))
    .orderBy('prediction')
)

preds_and_labels = preds_and_labels.select(['prediction','label'])
metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
print(metrics.confusionMatrix().toArray())

# COMMAND ----------

display(predictions)

# COMMAND ----------

print(preds_and_labels.where("prediction = 1").count())
print(preds_and_labels.where("label = 1").count())

# COMMAND ----------





# COMMAND ----------

mlflow.end_run()

# COMMAND ----------


