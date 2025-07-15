# Databricks notebook source
# base on jsl samples
# https://github.com/JohnSnowLabs/spark-nlp/blob/412ca982aa6cc286411c435dcd5fe1bc005659f5/examples/python/annotation/text/english/document-normalizer/document_normalizer_notebook.ipynb

# COMMAND ----------


#from pyspark.sql.types import StringType
import pyspark.sql.functions as F


spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #mandatory parameters and names. The orchestrator will always pass these
# MAGIC
# MAGIC headnode="fs_abfm_notes_partition"
# MAGIC stbl=dbutils.jobs.taskValues.get(taskKey=headnode,
# MAGIC                                  key="rawtable",
# MAGIC                                  default="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes",
# MAGIC                                  debug="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes",
# MAGIC                                  )
# MAGIC

# COMMAND ----------

## Cleaning functions
stbl=dbutils.jobs.taskValues.get("fs_abfm_notes_partition","basetable",debugValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes",)
txt="utf"
dbutils.widgets.text("enc",defaultValue="htm")
enc=dbutils.widgets.get("enc")

# COMMAND ----------

notes=spark.table(stbl).where(f"encoding='{enc}'") 
display(notes)
textcols=notes.schema.names


# COMMAND ----------


from datetime import date

cleanUpPatterns={
  "htm"     : ["<[^>]*>|&[^;]+;"],
  "htmnew"  : ["<[^>]*>|&[^;]+;"], 
  "xmlold"   : ["(?s)<script.*?(/>|</script>)", "<[^>]>", "(?U)<[^>]>"],
  "xmlbad"     : ["(?s)<script.*?(/>|)", "<[^>]>", "(?U)<[^>]>"],
  "xml"     : ["name"],
  "rtfCtrl" : ["/\n\\f[0-9]\s/g"],
  "rtfBasic": ["/\{\*?\\[^{}]+;}|[{}]|\\[A-Za-z]+\n?(?:-?\d+)?[ ]?/g"],
  "rtfnl"   : ["/\\\n/g"],
  "rtf":[],
                 }
cleanUpPattern=cleanUpPatterns[enc]
print(cleanUpPattern)

action="clean"
if(enc=="xml"):
  action="extract"

#    var basicRtfPattern = /\{\*?\\[^{}]+;}|[{}]|\\[A-Za-z]+\n?(?:-?\d+)?[ ]?/g;
#    var newLineSlashesPattern = /\\\n/g;
#    var ctrlCharPattern = /\n\\f[0-9]\s/g;

#    //Remove RTF Formatting, replace RTF new lines with real line breaks, and remove whitespace
#        .replace(ctrlCharPattern, "")
#        .replace(basicRtfPattern, "")
#        .replace(newLineSlashesPattern, "\n")
#        .trim();
#htmlCleanUpPatterns = ["<[^>]*>"]

#cleanUpPatterns = ["<[^>]*>|\\\\[^\\\\]+\\\\"]
#.setRules("<[^>]*>|\\\\[^\\\\]+\\\\") 

# COMMAND ----------

#setup sparknlp https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/sentence/sentence_detector/index.html
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader


# https://github.com/Dirkster99/PyNotes/blob/master/PySpark_SparkNLP/SentenceDetection/00%20Test%20Standard%20Sentence%20Detection.ipynb
documentAssembler = DocumentAssembler() \
    .setInputCol("note_text") \
    .setOutputCol("note_raw")


documentNormalizer = ( DocumentNormalizer() 
                       .setInputCols(["note_raw"]) 
                       .setOutputCol("note_doc") 
                       .setAction(action) 
                       .setPatterns(cleanUpPattern)
                       .setReplacement(" ") 
                       .setPolicy("pretty_all")
                       .setLowercase(False)
)

removerPipeline = Pipeline().setStages([
    documentAssembler,
    documentNormalizer,
#    keySql,
])
doc=removerPipeline.fit(notes).transform(notes)


# COMMAND ----------

print(stbl)
print(enc)
display(doc) 
doc.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stbl+'_raw__'+enc)



#this is a comment

# COMMAND ----------

cleandoc=doc.select(textcols+[F.explode(doc.note_doc.result).alias("result")])
display(cleandoc)
cleandoc.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stbl+'_'+enc)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/2.Text_Preprocessing_with_SparkNLP_Annotators_Transformers.ipynb
# MAGIC
# MAGIC documentAssembler = (
# MAGIC     DocumentAssembler()
# MAGIC     .setInputCol('note_clean_misc') 
# MAGIC     .setOutputCol('note_doc')
# MAGIC )
# MAGIC
# MAGIC #default
# MAGIC cleanUpPatternsHTML = ["<[^>]*>|&[^;]+;"]
# MAGIC documentNormalizerHTML = (
# MAGIC     DocumentNormalizer() 
# MAGIC     .setInputCols(["note_doc"]) 
# MAGIC     .setOutputCol("cleaned_markup") 
# MAGIC     .setAction("clean") 
# MAGIC     .setPatterns(cleanUpPatternsHTML) 
# MAGIC     .setReplacement(" ") 
# MAGIC     .setPolicy("pretty_all") 
# MAGIC     .setLowercase(True)
# MAGIC )
# MAGIC
# MAGIC sentence = (
# MAGIC     SentenceDetector() 
# MAGIC     .setInputCols(["cleaned_markup"]) 
# MAGIC     .setOutputCol("note_sent") 
# MAGIC     .setExplodeSentences(True)
# MAGIC ) 
# MAGIC
# MAGIC finisher = Finisher() \
# MAGIC     .setInputCols(["note_sent"]) \
# MAGIC     .setOutputCols(["note_string"]) \
# MAGIC     .setOutputAsArray(False) \
# MAGIC     .setCleanAnnotations(False) 
# MAGIC
# MAGIC
# MAGIC docPatternRemoverPipeline = (
# MAGIC     Pipeline() 
# MAGIC     .setStages([documentAssembler,                
# MAGIC                 documentNormalizerHTML,
# MAGIC                 sentence,
# MAGIC                 finisher
# MAGIC                 ]
# MAGIC                )
# MAGIC )
# MAGIC
# MAGIC treated_notes = docPatternRemoverPipeline.fit(notes).transform(notes)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC treated_notes.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC documentAssembler = DocumentAssembler()
# MAGIC .setInputCol("note_document")
# MAGIC .setOutputCol("note_doc")
# MAGIC
# MAGIC clean_up_patterns = ["(?s)<script.*?(/>|</script>)", "<[^>]>", "(?U)<[^>]>"]
# MAGIC
# MAGIC doc_norm = DocumentNormalizer()
# MAGIC .setInputCols("note_doc")
# MAGIC .setOutputCol("cleaned_markup")
# MAGIC .setAction("clean")
# MAGIC .setPatterns(clean_up_patterns)
# MAGIC .setReplacement(" ")
# MAGIC .setPolicy("pretty_all")
# MAGIC .setLowercase(False)
# MAGIC
# MAGIC pipeline = Pipeline().setStages([documentAssembler, doc_norm])
# MAGIC
# MAGIC pipelineDF = pipeline.fit(df).transform(df)

# COMMAND ----------

# MAGIC %md
# MAGIC import striprtf
# MAGIC rtf_id="6F61C832-1290-486D-B63E-BD0BE0A01A3E"
# MAGIC xml_id="3213f7ee-1a12-4214-ab9c-c84a1c69c514"
# MAGIC hex_id="5C250B00-0DA9-4054-B528-1D0886600C9E"
# MAGIC zzz_id="E91FEC3A-6F02-4FD2-AFAF-F305E6E4A594"
# MAGIC pipe_id="CCB68260-540C-4CC9-84D4-2E1DA3C2CD70"
# MAGIC html_id="f2368a09-41e1-4b0e-94e4-a6812b0bffb8"
# MAGIC patrec=spark.sql(f"select * from {stbl} where patient_id='{rtf_id}'").limit(1)
# MAGIC patrec.show()

# COMMAND ----------

# MAGIC %md ## Further cleaning 
# MAGIC
# MAGIC Cleaning ant html and rft within the text or partial matches of markup

# COMMAND ----------

# MAGIC %md
# MAGIC display(notes)
# MAGIC
# MAGIC #display(treated_notes.sort("person_id","note_datetime")) # sort seems to be an expensive operation
# MAGIC display(treated_notes) # sort seems to be an expensive operation

# COMMAND ----------

# MAGIC %md
# MAGIC treated_notes.selectExpr("explode(note_sent) as sentences").display()
# MAGIC treated_notes.display()
# MAGIC
# MAGIC #pipelineDF.display()
# MAGIC

# COMMAND ----------

#sentences=pipeline.fit(notes).transform(notes)

#treated_notes.selectExpr("explode(cleaned_markup) as test").show(truncate=False)

#sentences.display()#show(truncate=False)
#treated_notes_notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stbl+f"_cleaned")

#sentences.selectExpr("explode(note_sent) as sentences").show(truncate=False)

#sentences.display()#show(truncate=False)


# COMMAND ----------

#sentences.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-sentencizer

# COMMAND ----------


"""from spacy.lang.en import English

nlp = English()
nlp.add_pipe("sentencizer")

doc = nlp("This is a sentence. This is another sentence.")

rtf_notes=notes.withColumn("note_text",convertRTF(col("note_text")))
mpox_notes=notes.withColumn("mpox_notes",F.when(F.col('note_text').rlike(regex_pattern),True).otherwise(False))
mpox_notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stbl+f"_mpox")


 #add diagnosis here
"""
