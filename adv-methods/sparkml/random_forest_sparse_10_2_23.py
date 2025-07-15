# Databricks notebook source
import mlflow

#mlflow.autolog(
#    log_input_examples=False,
#    log_model_signatures=True,
#    log_models=True,
#    disable=False,
#    exclusive=True,
#    disable_for_unsupported_versions=True,
#    silent=True
#)

# COMMAND ----------

mlflow.set_experiment(experiment_id=1134367872147917)
mlflow.start_run() 
import mlflow.spark

# COMMAND ----------




# Maximize Pandas output text width.

import pandas as pd
from datetime import date
from itertools import chain
import math
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np


#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

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
    spark.table("cdh_abfm_phi_exploratory.ml_nlp_mpox_notes_presentation_bert_medical_ner_v2")
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

# MAGIC %sql
# MAGIC
# MAGIC select distinct entity_group from cdh_abfm_phi_exploratory.ml_nlp_mpox_notes_presentation_bert_medical_ner_v2
# MAGIC
# MAGIC

# COMMAND ----------

import sparkml

colums_to_select = [
    "patientuid",    
    (F.col("mpox_exposed")).alias("label"),
    "STI_encounter",
    #(F.col("diagnosistext")).alias("note_at_visit"),
    #(F.col("practiceids").getItem(0)).alias("practiceids"),
    "mpox_dates",
    #"socio_economic_chars",
    #"statecode",
    "age_group",
    "gender",
    #(F.col("note_date")).alias("note_around_visit"),
    "note_clean_misc",
    "entity_group",
    "word",
    "score"
    ]

#entity = ["disease_disorder"] # NER-all model
#entity = ['disease']# transformers scispacy model (deprecated), biobert
entity = ["i_problem", "b_problem"] # for bert medical

dbschema=f"cdh_abfm_phi_exploratory"
dataframe_temp = (
    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_nerall_entities_v2").select(*colums_to_select).sort("patientuid","STI_encounter")
    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_scispacy_entities").select(*colums_to_select).sort("patientuid","STI_encounter") deprecated
    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_biobert_diseases_ner_v2").select(*colums_to_select).sort("patientuid","STI_encounter")
    spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_bert_medical_ner_v2").select(*colums_to_select).sort("patientuid","STI_encounter")
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
    #.withColumn("practiceids", 
    #            F.col("practiceids").cast(StringType())
    #            )
             
)

dataframe_temp.select("patientuid").dropDuplicates().count()
dataframe_temp.select("patientuid").where("label = 1").dropDuplicates().count()

# COMMAND ----------

#original_DS = (
#    #spark.table("cdh_abfm_phi_exploratory.ml_nlp_mpox_notes_presentation_entities")     
#    spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_scispacy_entities")
#    #spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_biobert_diseases_ner")
#    .select(*colums_to_select)
#)

original_not_ner = (
    spark.table("cdh_abfm_phi_exploratory.mpox_data_set_presentation_v2_run9")
    .select("patientuid", "age_group", "gender", F.col("mpox_exposed").alias("label")) 
    #.withColumn("clean_non_alpha_num", F.lit('not_symthom_identified')) # not a symtom that is a variable
    .dropDuplicates()
)

display(original_not_ner)
original_not_ner.select("patientuid").dropDuplicates().count()

# COMMAND ----------

#original_DS.select("patientuid","label").where("label = 1").dropDuplicates().count()

# COMMAND ----------

#dataframe1.columns[0:5]

# COMMAND ----------

left_out = (
    original_not_ner.select("patientuid")    
    .join(dataframe_temp.select("patientuid"),
          ['patientuid'],
          'inner'
          )
)

display(left_out.dropDuplicates())

# COMMAND ----------

dataframe_temp.printSchema()

# COMMAND ----------

original_not_ner.columns

# COMMAND ----------

study_variables = ["patientuid", "age_group", "gender","label","clean_non_alpha_num"]

dataframe0 = (
    original_not_ner    
    .join(
        dataframe_temp.select("patientuid","clean_non_alpha_num"),
        ['patientuid'],
        'left'
    )    
    .select(*study_variables)
)

not_symthoms_detected_df = (
    #original_DS
    original_not_ner
    .withColumn("clean_non_alpha_num", F.lit("not_symthom_identified"))
    .select(*study_variables[0:6])
    .join(
        dataframe0.select("patientuid"),['patientuid'], 'leftanti'
    )
    
)

display(dataframe0)
dataframe0.select("patientuid").dropDuplicates().count()

# COMMAND ----------

dataframe1 = (
    dataframe0
    .drop("STI_encounter","mpox_dates")
    .withColumn('clean_non_alpha_num', F.regexp_replace(col('clean_non_alpha_num'), "\\.", ""))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='ca.', 'ca').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='pt.', 'pt').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='gender', 'gender_var').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='dr. david', 'dr_david').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='endoc.', 'endoc').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='dr. mongare', 'dr_mongare').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='ulnar nerve distribu.', 'ulnar nerve distribu').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num")=='a54.  gonococcal infection', 'a54 gonococcal infection').otherwise(F.col("clean_non_alpha_num")))
    .withColumn("clean_non_alpha_num", F.when(F.col("clean_non_alpha_num").isNull(), 'no_symthom_identified').otherwise(F.col("clean_non_alpha_num")))
    .sort("patientuid")    
    .unionByName(not_symthoms_detected_df)    
    .dropDuplicates()
    .groupBy(dataframe0.columns[0:4])    
    .pivot("clean_non_alpha_num")
    .count()
    .drop("monkey","monkeypox","monkeypox virus", "monkey pox", "monkey pox virus")
    #.drop("patientuid")
    .withColumnRenamed("features", "features_var")    
    .drop("statecode") # take in case of error  
)

columns_to_check = [col for col in dataframe1.columns if col not in dataframe1.columns[0:3]]

exprs = [F.when(F.col(col) > 0, 1).otherwise(0).alias(col) for col in columns_to_check]

dataframe = dataframe1.select(*dataframe1.columns[0:3], *exprs)

#display(not_symthoms_detected_df)
#print(not_symthoms_detected_df.dropDuplicates().count())
#display(dataframe)
#print(dataframe.dropDuplicates().count())

# COMMAND ----------

type(study_variables)

# COMMAND ----------

exprs

# COMMAND ----------

#print(colums_to_select)

# COMMAND ----------

#cat_variable = ["statecode", "age_group", "gender"]
cat_variable = ["age_group", "gender"]

dependent_variables = (
    dataframe.select(*cat_variable)              
)

indep_variable = ["label"]

db = dataframe.drop("practiceids","patientuid")

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

print("total number of patients with mpox", spark.table(f"{dbschema}.mpox_data_set_presentation_v2_run9").select("patientuid").dropDuplicates().count())

print("total number of patients with mpox", spark.table(f"{dbschema}.mpox_data_set_presentation_v2_run9").where("exposed =1").select("patientuid").dropDuplicates().count())

print("total number of patients that had a disease/ symthom identified with the ner", dataframe_temp.where("exposed =1").select("patientuid").dropDuplicates().count())

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

#display(temp_db2)

# COMMAND ----------

model_db = temp_db2.select(["features_rf","label"])

# COMMAND ----------

train_data, test_data = model_db.randomSplit([0.7, 0.3], seed=42)

# COMMAND ----------

#print("all data",model_db.count())
#print("train data",train_data.count())
#print("train data label",train_data.where("label =1").count())
#print("test data",test_data.count())
#print("test data label",test_data.where("label =1").count())

# COMMAND ----------

# for classification is recommended sqr(p) where p is the number of features that is 35 in

rf_model = RandomForestClassifier(labelCol = "label", numTrees = 30, featuresCol="features_rf", maxBins=60)
pipeline_parameter_exploration = Pipeline(stages=[rf_model]) 
#pipeline_parameter_exploration

# COMMAND ----------

#rf_classifier = (
#    rf_model
#    .fit(train_data)
#)

# COMMAND ----------

#print(rf_classifier.featureImportances)

# COMMAND ----------

#test_var_imp = rf_classifier.transform(train_data)
#display(test_var_imp)

# COMMAND ----------

#attrs = sorted(
#    (attr["idx"], attr["name"])
#    for attr in (
#        chain(*test_var_imp.schema["features_rf"].metadata["ml_attr"]["attrs"].values())
#    )
#) 


# COMMAND ----------

#var_imp_list = [(name, rf_classifier.featureImportances[idx]) for idx, name in attrs if rf_classifier.featureImportances[idx]]

# COMMAND ----------

#sorted_data_list = sorted(var_imp_list, key = lambda x: x[1], reverse=True)
#top_words = [item[0] for item in sorted_data_list[:round(math.sqrt(len(dataframe.columns)))]]

# COMMAND ----------

#top_words

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

#BestModel= cvModel.bestModel
#BestModel

# COMMAND ----------

#type(BestModel)

# COMMAND ----------

best_rf_model = cvModel.bestModel.stages[-1]
importances = best_rf_model.featureImportances.toArray()



# COMMAND ----------

classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 30, maxBins=60)
paramGrid = (ParamGridBuilder()\
            .addGrid(classifier.maxDepth, [10])
            .addGrid(classifier.maxBins, [20])
            .addGrid(classifier.numTrees,[50])
            .build())
crossval = CrossValidator(estimator=classifier,
                         estimatorParamMaps=paramGrid,
                         evaluator=MulticlassClassificationEvaluator(),
                         numFolds=2)
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

# source https://antonhaugen.medium.com/feature-selection-for-pyspark-tree-classifiers-3be54a8bc493

results = spark.createDataFrame([],schema)
MC_evaluator = MulticlassClassificationEvaluator(metricName="f1")

paramGrid = (
        ParamGridBuilder()
        .addGrid(classifier.maxDepth, [10])
        .addGrid(classifier.maxBins, [20])
        .addGrid(classifier.numTrees,[50])
        .build()
    )

crossval = (
    CrossValidator(
        estimator=classifier,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=2)
)

classifier = RandomForestClassifier(featuresCol='features_rf', numTrees = 30, maxBins=60)
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


