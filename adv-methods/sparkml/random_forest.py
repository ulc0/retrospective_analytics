# Databricks notebook source


# COMMAND ----------


# Maximize Pandas output text width.
import pandas as pd
from datetime import date
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np


#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import SparkSession

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

spark.sql("SHOW DATABASES").show(truncate=False)

#import pyspark sql functions with alias
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp, greatest, least, ceil, expr
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession, DataFrame



spark.sql("SHOW DATABASES").show(truncate=False)
spark.conf.set('spark.sql.shuffle.partitions',7200*4)


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

entity = ["disease_disorder"]


dbschema=f"cdh_abfm_phi_exploratory"
dataframe = (
    spark.table(f"{dbschema}.ml_nlp_mpox_notes_entities").select(*colums_to_select).sort("patientuid","STI_encounter")
    .filter(F.col("entity_group").isin(entity))
    .withColumn("clean_non_alpha_num0",
                F.regexp_replace(F.col("word"), r"#|/|\[|\]|\(|\[^0-9]","") 
                )
    .withColumn("clean_non_alpha_num1",
                F.regexp_replace(F.col("clean_non_alpha_num0"), r"\||-","") 
                )
    .withColumn("clean_non_alpha_num2",
                F.regexp_replace(F.col("clean_non_alpha_num1"), r"\b\d+\b","") 
                )
    .withColumn("clean_non_alpha_num",
                F.when(
                    F.col('clean_non_alpha_num2').like('%e11%'), F.lit("")
                    )
                .otherwise(F.col('clean_non_alpha_num2'))
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

#dataframe.display()

#dataframe.select("patientuid").dropDuplicates().count()



# COMMAND ----------

#display(dataframe)
#print((dataframe.count(), len(dataframe.columns)))

# COMMAND ----------

print(colums_to_select)

# COMMAND ----------



cat_variable = ["practiceids","statecode", "age_group", "gender","clean_non_alpha_num"]

dependent_variables = (
    dataframe.select(*cat_variable)
              
)

dep_variable = ["label"]

db = dataframe.select(*cat_variable,*dep_variable)

# COMMAND ----------

#display(dependent_variables)
#print((dependent_variables.count(), len(dependent_variables.columns)))

display(
    dependent_variables
    .select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dependent_variables.columns])
    )

# COMMAND ----------

dependent_variables.columns[2:4]

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

indexers = (
    [StringIndexer(inputCol=column, outputCol=column+"_index").fit(db) for column in list(set(dependent_variables.columns)) ]
)

pipeline = Pipeline(stages=indexers)
db2 = pipeline.fit(db).transform(db)

# COMMAND ----------

db2.columns[6:11]

# COMMAND ----------

display(db2)

# COMMAND ----------

assembler = VectorAssembler( inputCols = db2.columns[6:11], outputCol = "features" )

# COMMAND ----------

assembler

# COMMAND ----------

output = assembler.transform(db2)
display(output)

# COMMAND ----------

model_df = output.select(["features","label"])
display(model_df)

# COMMAND ----------

train_data, test_data = model_df.randomSplit([0.7, 0.3], seed=42)

# COMMAND ----------

train_data.count()

# COMMAND ----------

rf_model = RandomForestClassifier(labelCol = "label", numTrees = 30, featuresCol="features", maxBins=1201)
pipeline_parameter_exploration = Pipeline(stages=[rf_model]) 
rf_classifier = (
    rf_model
    .fit(train_data)
)

# COMMAND ----------

rf_predictions = rf_classifier.transform(test_data)

# COMMAND ----------

rf_predictions.display()

# COMMAND ----------

print(train_data.count())
print(test_data.count())

# COMMAND ----------

rf_auc = BinaryClassificationEvaluator(labelCol = "label").evaluate(rf_predictions)
rf_auc

# COMMAND ----------

rf_classifier.featureImportances

# COMMAND ----------

# MAGIC %md Mapping back to classes

# COMMAND ----------

# source https://stackoverflow.com/questions/50937591/pyspark-random-forest-feature-importance-mapping-after-column-transformations

# COMMAND ----------

from itertools import chain

attrs = sorted(
    (attr["idx"], attr["name"])
    for attr in (
        chain(*rf_predictions.schema["features"].metadata["ml_attr"]["attrs"].values())
    )
) 

# COMMAND ----------

[
    (name, rf_classifier.featureImportances[idx])
    for idx, name in attrs
    if rf_classifier.featureImportances[idx]
]

# COMMAND ----------

display(rf_predictions.select("features", "label", "prediction", "probability"))

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

cvModel = cv.fit(train_data)
cvModel

# COMMAND ----------

best_model = cvModel.bestModel
best_model

# COMMAND ----------

# Use the model identified by the cross-validation to make predictions on the test dataset
cvPredDF = best_model.transform(test_data)
cvPredDF 


# COMMAND ----------

display(cvPredDF)

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
