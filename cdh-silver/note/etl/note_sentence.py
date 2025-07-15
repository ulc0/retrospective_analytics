# Databricks notebook source
# mandatory parameters and names. The orchestrator will always pass these

dbutils.widgets.text("CLEAN_TABLE", defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_mpox_cohort_notes_clean") #,patientnoteproblem,patientnoteresultobservation") #"patientnoteproblem,patientnoteresultobservation")
dbutils.widgets.text("EXPERIMENT_ID", defaultValue="1504400005030765")
dbutils.widgets.text("SENTENCE_TABLE", defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_mpox_cohort_notes_sentences")

# COMMAND ----------



# COMMAND ----------

EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")
notetable = dbutils.widgets.get("CLEAN_TABLE")
print(notetable)
output_table=dbutils.widgets.get("SENTENCE_TABLE")
print(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC #TODO match source version
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC table_path = "/path/to/your/delta/table"
# MAGIC deltaTable = DeltaTable.forPath(spark, table_path)
# MAGIC
# MAGIC history_df = deltaTable.history() \
# MAGIC     .select("version") \
# MAGIC     .orderBy("version", ascending=False)
# MAGIC
# MAGIC versions = history_df.collect()[0]
# MAGIC version=versions[0]
# MAGIC
# MAGIC df = spark.read \
# MAGIC     .format("delta") \
# MAGIC     .option("versionAsOf",version) \
# MAGIC     .load(delta_table_path)
# MAGIC ```

# COMMAND ----------

import pyspark.sql.functions as F
import shared.text_process as text_process
import os
os.environ["PYSPARK_PIN_THREAD"]="False"
#import mlflow
#import mlflow.spark

#mlflow.spark.autolog()
#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
#mlflow.end_run()
#run=mlflow.start_run(experiment_id=EXPERIMENT_ID,run_name=run_name)
#run_id=run.info.run_id
#dbutils.jobs.taskValues.set("run_id",run_id)
#spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------




import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType
#import re
import shared.text_process as text_process

# rtf works from striprtf.striprtf import rtf_to_text


# COMMAND ----------

print(notetable)

# COMMAND ----------

notes=spark.table(notetable) # for testing.limit(1000) 
display(notes)

# COMMAND ----------

from pyspark.ml import Pipeline
#setup sparknlp https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/sentence/sentence_detector/index.html
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.common import *


# COMMAND ----------

# MAGIC %md
# MAGIC ## setCustomBounds() : Custom sentence separator text e.g. ["\n"]
# MAGIC
# MAGIC setUseCustomOnly() : Use only custom bounds without considering those of Pragmatic Segmenter. Defaults to false. Needs customBounds.
# MAGIC
# MAGIC setUseAbbreviations : Whether to consider abbreviation strategies for better accuracy but slower performance. Defaults to true.
# MAGIC
# MAGIC setExplodeSentences : Whether to split sentences into different Dataset rows. Useful for higher parallelism in fat rows. Defaults to false.

# COMMAND ----------


# https://github.com/Dirkster99/PyNotes/blob/master/PySpark_SparkNLP/SentenceDetection/00%20Test%20Standard%20Sentence%20Detection.ipynb
documentAssembler = DocumentAssembler() \
    .setInputCol("clean_text") \
    .setOutputCol("text") \
   # .setIdCol("note_id") \
   # .setCleanupMode("shrink_full") 
sentenceDetector = SentenceDetector()\
      .setInputCols(['text'])\
      .setOutputCol('sentences')\
      #.setExplodeSentences(true)
sentenceDetector.extractParamMap()


# COMMAND ----------

sentencePipeline = Pipeline().setStages([
    documentAssembler,
        sentenceDetector,
])
doc=sentencePipeline.fit(notes).transform(notes)

# COMMAND ----------

display(doc)

# COMMAND ----------

doc.printSchema()

# COMMAND ----------

display(doc.select("note_id",F.posexplode("sentences")).select("note_id","pos","col.*"))

# COMMAND ----------

renameCols={"result":"note_text",
            "pos":"note_sent",}
sentences=doc.select("note_id",F.posexplode("sentences")).select("note_id","pos","col.*").drop("metadata","embeddings").withColumnsRenamed(renameCols).drop("annotatorType").transform(text_process.count_words, "note_text")

# COMMAND ----------

#display(sentences) #.select("*",'col.*'))

# COMMAND ----------

#TODO versioning here? Follow CDH Best Practices
sentences.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)



