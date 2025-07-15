# Databricks notebook source
import mlflow

mlflow.autolog(
    log_input_examples=False,
    log_model_signatures=True,
    log_models=True,
    disable=False,
    exclusive=True,
    disable_for_unsupported_versions=True,
    silent=True
)

# COMMAND ----------


mlflow.set_experiment(experiment_id=1134367872147917)
mlflow.start_run() 
# Maximize Pandas output text width.
import mlflow.spark
import pandas as pd
from datetime import date
from itertools import chain
import math
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np


#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

#import pyspark sql functions with alias
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp, greatest, least, ceil, expr
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession, DataFrame



#spark.sql("SHOW DATABASES").show(truncate=False)
spark.conf.set('spark.sql.shuffle.partitions',7200*4)


# COMMAND ----------

notes_stats = (
    spark.table("cdh_abfm_phi_exploratory.ml_nlp_mpox_notes_presentation_biobert_diseases_ner")
    .select("patientuid","mpox_exposed","notes","STI_encounter") 
    .dropDuplicates() 
    .withColumn("note_lenght",
                F.length(F.col("notes"))
                )      
)
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

# COMMAND ----------

# checking for scispacy entities ner
display(spark.table("cdh_abfm_phi_exploratory.ml_nlp_mpox_notes_presentation_biobert_diseases_ner").where("entity_group == 'disease'"))

# COMMAND ----------

import sparkml

colums_to_select = [
    "patientuid",    
    (F.col("mpox_exposed")).alias("label"),
    "STI_encounter",
    (F.col("diagnosistext")).alias("note_at_visit"),
    (F.col("practiceids").getItem(0)).alias("practiceids"),
    "mpox_dates",
    "socio_economic_chars",
    "statecode",
    "age_group",
    "gender",
    (F.col("note_date")).alias("note_around_visit"),
    "note_clean_misc",
    "entity_group",
    "word",
    "score"
    ]

#entity = ["disease_disorder"] # NER-all model
entity = ['disease']# transformers scispacy model

dbschema=f"cdh_abfm_phi_exploratory"
dataframe_temp = (
    spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_entities").select(*colums_to_select).sort("patientuid","STI_encounter")
    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_scispacy_entities").select(*colums_to_select).sort("patientuid","STI_encounter")
    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_biobert_diseases_ner").select(*colums_to_select).sort("patientuid","STI_encounter")
    .filter(F.col("entity_group").isin(entity))
    .withColumn("clean_non_alpha_num0",
                F.regexp_replace(F.col("word"), r"#|/|\[|\]|\(|\[^0-9]","") 
                )
    .withColumn("clean_non_alpha_num1",
                F.regexp_replace(F.col("clean_non_alpha_num0"), r"\||-|" ,"") 
                )
    .withColumn("clean_non_alpha_num2",
                F.regexp_replace(F.col("clean_non_alpha_num1"), r"\b\d+\b","") 
                )
    .withColumn("clean_non_alpha_num3",
                F.regexp_replace(F.col("clean_non_alpha_num2"), r"[|&*]","") 
                )
    .withColumn("clean_non_alpha_num",
                F.when(
                    F.col('clean_non_alpha_num3').like('%e11%'), F.lit("")
                    )
                .otherwise(F.col('clean_non_alpha_num3'))
                )
    .where(F.col("clean_non_alpha_num")!='')
    .withColumn("practiceids", 
                F.col("practiceids").cast(StringType())
                )
    .withColumn("statecode",
                F.when(F.col("statecode").isNull(), "not_avaiable")
                .otherwise(F.col("statecode"))    
                )           
)


#display(
#    dataframe
#    .groupBy("entity_group","word","clean_non_alpha_num")
#    .count()
#)



#dataframe.select("patientuid").dropDuplicates().count()



# COMMAND ----------

study_variables = ["patientuid","practiceids","statecode", "age_group", "gender","label","clean_non_alpha_num"]

dataframe0 = (
    dataframe_temp
    .select(*study_variables)
    .groupBy("patientuid","practiceids","statecode", "age_group", "gender")
    .agg(
        F.array_distinct((F.collect_list("clean_non_alpha_num"))).alias("clean_non_alpha_num"),
        (F.array_max(F.collect_list("label"))).alias("label")
    )
    .withColumn("clean_non_alpha_num",
                F.explode("clean_non_alpha_num")
                )
    .select(*study_variables)
    
)

dataframe1 = (
    dataframe0
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='ca.', 'ca').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='pt.', 'pt').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='gender', 'gender_var').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='dr. david', 'dr_david').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='dr. mongare', 'dr_mongare').otherwise(F.col("clean_non_alpha_num")))
    .sort("patientuid")
    .groupBy(dataframe0.columns[0:6])    
    .pivot("clean_non_alpha_num")
    .count()
    .drop("patientuid","monkey","monkeypox","monkeypox virus", "monkey pox", "monkey pox virus")
    .withColumnRenamed("features", "features_var")
    
)

columns_to_check = [col for col in dataframe1.columns if col not in dataframe1.columns[0:5]]

exprs = [F.when(F.col(col) > 0, 1).otherwise(0).alias(col) for col in columns_to_check]

dataframe = dataframe1.select(*dataframe1.columns[0:5], *exprs)



# COMMAND ----------

dataframe1.printSchema()

# COMMAND ----------

exprs

# COMMAND ----------

#print(colums_to_select)

# COMMAND ----------



cat_variable = ["statecode", "age_group", "gender"]

dependent_variables = (
    dataframe.select(*cat_variable)              
)

indep_variable = ["label"]

db = dataframe.drop("practiceids")

# COMMAND ----------



# COMMAND ----------

display(dependent_variables)
print((dependent_variables.count(), len(dependent_variables.columns)))

#display(
#    dataframe
#    .select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns])
#    )

# COMMAND ----------

len(db.columns) 

# COMMAND ----------

print("total number of patients with mpox", spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_entities").select("patientuid").dropDuplicates().count())

print("total number of patients with mpox", spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_entities").where("exposed =1").select("patientuid").dropDuplicates().count())

print("total number of patients that had a disease/ symthom identified with the ner", dataframe_temp.where("exposed =1").select("patientuid").dropDuplicates().count())

# COMMAND ----------

db.where("label =1").count()

# COMMAND ----------

#display(db)

#from pyspark.sql.functions import col, countDistinct

#display(db.agg(*(countDistinct(col(c)).alias(c) for c in db.columns)))

# COMMAND ----------



# COMMAND ----------

db.columns[4:]

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

indexers = ( # transforming cathegorical vars into numbers
    [StringIndexer(inputCol=column, outputCol=column+"_index").fit(db) for column in list(set(dependent_variables.columns)) ]
)

pipeline = Pipeline(stages=indexers)
temp_db = pipeline.fit(db).transform(db)

# COMMAND ----------

temp_db.columns[5:]

# COMMAND ----------

# assembling all feature vectors that are not in the indexer
assembler = VectorAssembler(inputCols = temp_db.columns[5:], outputCol = "features_rf" ) # there is a column 'word' called feature
temp_db2 = assembler.transform(temp_db)


# COMMAND ----------

display(temp_db2)

# COMMAND ----------

model_db = temp_db2.select(["features_rf","label"])

# COMMAND ----------

#model_db = assembler.transform(temp_db).select(["features","label"]) # putting everything together in the right format

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

rf_model = RandomForestClassifier(labelCol = "label", numTrees = 30, featuresCol="features_rf", maxBins=60)
pipeline_parameter_exploration = Pipeline(stages=[rf_model]) 
pipeline_parameter_exploration

# COMMAND ----------

rf_classifier = (
    rf_model
    .fit(train_data)
)

# COMMAND ----------

print(rf_classifier.featureImportances)

# COMMAND ----------

test_var_imp = rf_classifier.transform(train_data)
display(test_var_imp)

# COMMAND ----------

attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*test_var_imp.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
    )
) 


# COMMAND ----------

var_imp_list = [(name, rf_classifier.featureImportances[idx]) for idx, name in attrs if rf_classifier.featureImportances[idx]]

# COMMAND ----------

sorted_data_list = sorted(var_imp_list, key = lambda x: x[1], reverse=True)
top_words = [item[0] for item in sorted_data_list[:round(math.sqrt(len(dataframe.columns)))]]

# COMMAND ----------

top_words

# COMMAND ----------

#Define the hyperparameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_model.numTrees, [10, 30, 50, 80])
    .build()
)
paramGrid

# COMMAND ----------

cv = CrossValidator(
    estimator=pipeline_parameter_exploration, 
    estimatorParamMaps=paramGrid, 
    evaluator=BinaryClassificationEvaluator(), 
    numFolds=3, 
    )
 
# Run cross validations. This step takes a few minutes and returns the best model found from the cross validation.
cv

# COMMAND ----------

cvModel = cv.fit(train_data)
#cvModel = cv.fit()
cvModel

# COMMAND ----------

BestModel= cvModel.bestModel
BestModel

# COMMAND ----------

type(BestModel)

# COMMAND ----------

best_rf_model = cvModel.bestModel.stages[-1]
importances = best_rf_model.featureImportances.toArray()



# COMMAND ----------

display(importances)

# COMMAND ----------

# source https://antonhaugen.medium.com/feature-selection-for-pyspark-tree-classifiers-3be54a8bc493

maximum  = 1204
n = 35

classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 30, maxBins=60)
for n in range(10, maximum, 10):
    print("Testing top n= ", n, " features")
    
    best_n_features= importances.argsort()[-n:][::-1]
    best_n_features= best_n_features.tolist()
    vs= VectorSlicer(inputCol='features_rf',   outputCol='best_features',indices=best_n_features)
    bestFeaturesDf= vs.transform(model_db)
    train,test= bestFeaturesDf.randomSplit([0.7, 0.3])
    
    columns = ['Classifier', 'Result']
    vals = [('Place Holder', 'N/A')]
    results = spark.createDataFrame(vals, columns)
    
    paramGrid = (ParamGridBuilder()\
            .addGrid(classifier.maxDepth, [2, 5, 10])
            .addGrid(classifier.maxBins, [40, 60 , 80])
            .addGrid(classifier.numTrees,[5, 30, 60])
            .build())


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

crossval = CrossValidator(estimator=classifier,
                         estimatorParamMaps=paramGrid,
                         evaluator=BinaryClassificationEvaluator(),
                         numFolds=5)
fitModel = crossval.fit(train)



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

attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*predictions.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
    )
) 


# COMMAND ----------

var_imp_list = [(name, rf_classifier.featureImportances[idx]) for idx, name in attrs if rf_classifier.featureImportances[idx]]

# COMMAND ----------

sorted_data_list = sorted(var_imp_list, key = lambda x: x[1], reverse=True)
top_words = [item[0] for item in sorted_data_list[:round(math.sqrt(len(dataframe.columns)))]] # rounding to the sqr of the initial number of features



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
model = rf_classifier
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

"""import matplotlib.pyplot as plt

# Create a Pipeline estimator and fit on train DF, predict on test DF
model = estimator.fit(train)
predictions = model.transform(test)

# Returns as a list (false positive rate, true positive rate)
preds = predictions.select('label','probability').rdd.map(lambda row: (float(row['probability'][1]), float(row['label'])))
points = CurveMetrics(preds).get_curve('roc')

plt.figure()
x_val = [x[0] for x in points]
y_val = [x[1] for x in points]
plt.title('ROC curve')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.plot(x_val, y_val)"""

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

rf_classifier = (
    rf_model
    .fit(train_data)
)

# COMMAND ----------

rf_classifier

# COMMAND ----------

print(rf_classifier.featureImportances)

# COMMAND ----------

test_var_imp = rf_classifier.transform(train_data)
#display(test_var_imp)

# COMMAND ----------

# MAGIC %md Feature importance, train data

# COMMAND ----------

attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*test_var_imp.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
    )
) 

#type(attrs)

# COMMAND ----------

var_imp_list = [(name, rf_classifier.featureImportances[idx]) for idx, name in attrs if rf_classifier.featureImportances[idx]]

# COMMAND ----------

sorted_data_list = sorted(var_imp_list, key = lambda x: x[1], reverse=True)
top_words = [item[0] for item in sorted_data_list[:round(math.sqrt(len(dataframe.columns)))]] # rounding to the sqr of the initial number of features



# COMMAND ----------

rf_predictions = rf_classifier.transform(test_data[[top_words]])

# COMMAND ----------

rf_predictions.display()

# COMMAND ----------

# MAGIC %md Feature importance test data

# COMMAND ----------


attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*rf_predictions.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
    )
) 

# COMMAND ----------

var_imp_list_test = [ (name, rf_classifier.featureImportances[idx]) for idx, name in attrs if rf_classifier.featureImportances[idx]]

# COMMAND ----------

sorted_data_list2 = sorted(var_imp_list_test, key = lambda x: x[1], reverse=True)
top_words2 = [item[0] for item in sorted_data_list2[:round(math.sqrt(len(dataframe.columns)))]]

# COMMAND ----------



# COMMAND ----------

print(train_data.count())
print(test_data.count())

# COMMAND ----------

rf_auc = BinaryClassificationEvaluator(labelCol = "label").evaluate(rf_predictions)
rf_auc

# COMMAND ----------

# source https://stackoverflow.com/questions/50937591/pyspark-random-forest-feature-importance-mapping-after-column-transformations

# COMMAND ----------

display(rf_predictions.select("features_rf", "label", "prediction", "probability"))

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
model = rf_classifier
predictions = rf_predictions

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

 
bcEvaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
print(f"Area under ROC curve: {bcEvaluator.evaluate(predictions)}")
 
mcEvaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print(f"Accuracy: {mcEvaluator.evaluate(predictions)}")

# COMMAND ----------

#Define the hyperparameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_model.numTrees, [10, 30, 50, 80])
    .build()
)
paramGrid

# COMMAND ----------

# Create a 3-fold CrossValidator
cv = CrossValidator(
    estimator=pipeline_parameter_exploration, 
    estimatorParamMaps=paramGrid, 
    evaluator=BinaryClassificationEvaluator(), 
    numFolds=3, 
    )
 
# Run cross validations. This step takes a few minutes and returns the best model found from the cross validation.
cv

# COMMAND ----------

cvModel = cv.fit(train_data[top_words])
cvModel

# COMMAND ----------

best_model = cvModel.bestModel
best_model

# COMMAND ----------

print(best_model)

# COMMAND ----------

print(zip(cvModel.avgMetrics, paramGrid))

# COMMAND ----------

your_model_rf = cvModel.bestModel.stages[-1] 
var_imp = your_model_rf.featureImportances


# COMMAND ----------

results_best_model =  [(name, your_model_rf.featureImportances[idx]) for idx, name in attrs if your_model_rf.featureImportances[idx]]

# COMMAND ----------

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

# COMMAND ----------



# COMMAND ----------

best_words = pd.DataFrame(results_best_model, columns =['feature', 'coefficient']).sort_values("coefficient", ascending=False)
print(best_words)

# COMMAND ----------



# COMMAND ----------

# Use the model identified by the cross-validation to make predictions on the test dataset
cvPredDF = best_model.transform(test_data)
cvPredDF 


# COMMAND ----------

display(cvPredDF)

# COMMAND ----------

cvModel.bestModel.stages

# COMMAND ----------

print(cvModel.bestModel.stages)

# COMMAND ----------

# Evaluate the model's performance based on area under the ROC curve and accuracy 
print(f"Area under ROC curve: {bcEvaluator.evaluate(cvPredDF)}")
print(f"Accuracy: {mcEvaluator.evaluate(cvPredDF)}")

# COMMAND ----------


from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

preds_and_labels = (
    cvPredDF
    .select(['prediction','label'])
    .withColumn('label', F.col('label').cast(FloatType()))
    .orderBy('prediction')
)

preds_and_labels = preds_and_labels.select(['prediction','label'])
metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
print(metrics.confusionMatrix().toArray())

# COMMAND ----------

# ToDO
# Create confusion matrix for spark
# UMLS normalization run
# Return the terms that give better prediction in the symthoms variable or make matrix very sparse.
## Check on another ner model spacy

##nlp=spacy.load('en_core_web_sm')
#nlp.pipe_names
#['tok2vec', 'tagger', 'parser', 'attribute_ruler', 'lemmatizer', 'ner']

# COMMAND ----------

"""from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

def mapper(line):
    label = pivoted_df.select("mpox_exposed")
    features =  pivoted_df.drop("mpox_exposed")
    return LabeledPoint(label, features)

# Load and parse the data file into an RDD of LabeledPoint.
#data = MLUtils.loadLibSVMFile(sc, pivoted_df)
data = mapper()
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
#  Note: Use larger numTrees in practice.
#  Setting featureSubsetStrategy="auto" lets the algorithm choose.
model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(
    lambda lp: lp[0] != lp[1]).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())

# Save and load model
#model.save(sc, "target/tmp/myRandomForestClassificationModel")
#sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
"""
