# Databricks notebook source
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------


ttbl="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes"
basetable=dbutils.jobs.taskValues.get('fs_abfm_notes_partition','basetable',default=ttbl,debugValue=ttbl)
print(basetable)
import mlflow.spark
print(mlflow.__version__)
#mlflow.spark.autolog()

# COMMAND ----------

extensions=["rtf_fcn","xml","htm"]
sqlstring=f"select * from {basetable}_rtf_fcn UNION "+ \
    f"select * from {basetable}_xml UNION "+ \
    f"select * from {basetable}_htm UNION "+ \
    f"select *,note_text as result from {basetable} where encoding not in ('rtf','xml','htm') ORDER BY encoding,person_id,note_datetime" 
    #"UNION select * from {basetable}_xml",
print(sqlstring)
#select *,note_text as result  from hive_metastore.edav_prd_cdh.edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes where encoding not in ('rtf','xml','htm') ORDER BY encoding,person_id,note_datetime

# COMMAND ----------

# MAGIC %md
# MAGIC [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The 12th column of the second table is "ARRAY<STRING>" type which is not compatible with "STRING" at the same column of the first table.. SQLSTATE: 42825
# MAGIC File <command-3690049354522058>, line 1
# MAGIC ----> 1 notes=spark.sql(sqlstring)
# MAGIC File /databricks/spark/python/pyspark/errors/exceptions/captured.py:230, in capture_sql_exception.<locals>.deco(*a, **kw)
# MAGIC     226 converted = convert_exception(e.java_exception)
# MAGIC     227 if not isinstance(converted, UnknownException):
# MAGIC     228     # Hide where the exception came from that shows a non-Pythonic
# MAGIC     229     # JVM exception message.
# MAGIC --> 230     raise converted from None
# MAGIC     231 else:
# MAGIC     232     raise

# COMMAND ----------

notes=spark.sql(sqlstring) #.join(sti_patientuid,["person_id"])

# COMMAND ----------

cols=list(notes.columns)[:-1] # skip result/last column
print(cols)
sentence_column="note_sent"
finalcolumn=["note_string"]
cols=cols+finalcolumn+[sentence_column]
print(cols)


# COMMAND ----------

import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import DocumentNormalizer
from sparknlp.annotator.sentence import SentenceDetector
from pyspark.ml import Pipeline
from sparknlp.common import *

# COMMAND ----------

#https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/2.Text_Preprocessing_with_SparkNLP_Annotators_Transformers.ipynb

documentAssembler = (
    DocumentAssembler()
    .setInputCol('result') 
    .setOutputCol('note_doc')
)

#default
cleanUpPatternsHTML = ["<[^>]*>|&[^;]+;"]
documentNormalizerHTML = (
    DocumentNormalizer() 
    .setInputCols(["note_doc"]) 
    .setOutputCol("cleaned") 
    .setAction("clean") 
    .setPatterns(cleanUpPatternsHTML) 
    .setReplacement(" ") 
    .setPolicy("pretty_all") 
    .setLowercase(True)
)

sentence = (
    SentenceDetector() 
    .setInputCols(["cleaned"]) 
    .setOutputCol(sentence_column) 
    .setExplodeSentences(True)
) 

finisher = Finisher() \
    .setInputCols([sentence_column]) \
    .setOutputCols(finalcolumn) \
    .setOutputAsArray(False) \
    .setCleanAnnotations(False) \
  


docPatternRemoverPipeline = (
    Pipeline() 
    .setStages([documentAssembler,                
                documentNormalizerHTML,
                sentence,
                finisher
                ]
               )
)

# COMMAND ----------

treated_notes = docPatternRemoverPipeline.fit(notes).transform(notes)

# COMMAND ----------

import pyspark.sql.functions as F

display(treated_notes.select(cols)) #.select(F.posexplode_outer("note_sent")).collect())


# COMMAND ----------


otbl=f"{basetable}_text"
dbutils.jobs.taskValues.set(key="texttable",value=otbl)
treated_notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl)

# COMMAND ----------

for ext in extensions:
    spark.sql(f"drop table {basetable}_{ext}")

# COMMAND ----------

# MAGIC %md
# MAGIC def deleteMarkup(colNote):        
# MAGIC      rtf = F.regexp_replace(colNote, r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","") 
# MAGIC      xlm = F.regexp_replace(rtf, r"<[^>]*>|&[^;]+;", "")
# MAGIC      font_header1 = F.regexp_replace(xlm, "Tahoma;;", "")
# MAGIC      font_header2 = F.regexp_replace(font_header1, "Arial;Symbol;| Normal;heading 1;", "")
# MAGIC      font_header3 = F.regexp_replace(font_header2, "Segoe UI;;", "")
# MAGIC      font_header4 = F.regexp_replace(font_header3, "MS Sans Serif;;", "")
# MAGIC      return font_header4       
# MAGIC
# MAGIC
