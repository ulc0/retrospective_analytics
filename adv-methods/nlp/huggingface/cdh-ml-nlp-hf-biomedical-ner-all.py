# Databricks notebook source
#%pip install spark-nlp==5.1.3
#%pip install spark-nlp==4.3.2

# COMMAND ----------

#pyspark --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.3

# COMMAND ----------

#spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from datetime import date
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
from pyspark.sql.functions import pandas_udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from functools import reduce

from pyspark.sql.types import StringType,  ArrayType
from pyspark.sql.functions import pandas_udf, udf, PandasUDFType

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

#temporarily here the spark nlp

# COMMAND ----------



notes0 = spark.table("cdh_abfm_phi_exploratory.mpox_data_set_presentation_v2_run9")
#spark.sql("SHOW DATABASES").show(truncate=False)
#import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, lower, udf
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
#   RTF from striprtf.striprtf import rtf_to_text

spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
#setup sparknlp https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/sentence/sentence_detector/index.html

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader
#https://medium.com/spark-nlp/cleaning-and-extracting-content-from-html-xml-documents-using-spark-nlp-documentnormalizer-913d96b2ee34


# cleaning function for misc cleaning
def deleteMarkup(colNote):                  
     font_header1 = regexp_replace(colNote, "Tahoma;;", "")
     font_header2 = regexp_replace(font_header1, "Arial;Symbol;| Normal;heading 1;", "")
     font_header3 = regexp_replace(font_header2, "Segoe UI;;", "")
     font_header4 = regexp_replace(font_header3, "MS Sans Serif;;", "")
     return font_header4 

import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType


clean_text = ( 
              # cleaning RFT here, document normalizer does not capture the RTF pattern. It is easier doing it here instead
    lambda s: re.sub(r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?", '', s)
)

notes = (
    notes0
    .withColumn('note_clean_rtf', udf(clean_text, StringType())('notes'))
    .withColumn('note_clean_misc', deleteMarkup(F.col("note_clean_rtf")))
)


#https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/2.Text_Preprocessing_with_SparkNLP_Annotators_Transformers.ipynb

documentAssembler = (
    DocumentAssembler()
    .setInputCol('note_clean_misc') 
    .setOutputCol('note_doc')
)

#default
cleanUpPatternsHTML = ["<[^>]*>|&[^;]+;"]
documentNormalizerHTML = (
    DocumentNormalizer() 
    .setInputCols(["note_doc"]) 
    .setOutputCol("cleaned_markup") 
    .setAction("clean") 
    .setPatterns(cleanUpPatternsHTML) 
    .setReplacement(" ") 
    .setPolicy("pretty_all") 
    .setLowercase(True)
)

sentence = (
    SentenceDetector() 
    .setInputCols(["cleaned_markup"]) 
    .setOutputCol("note_sent") 
    .setExplodeSentences(True)
) 

finisher = Finisher() \
    .setInputCols(["note_sent"]) \
    .setOutputCols(["note_string"]) \
    .setOutputAsArray(False) \
    .setCleanAnnotations(False) 


docPatternRemoverPipeline = (
    Pipeline() 
    .setStages([documentAssembler,                
                documentNormalizerHTML,
                sentence,
                finisher
                ]
               )
)

treated_notes = docPatternRemoverPipeline.fit(notes).transform(notes)
display(notes)
#display(treated_notes.sort("person_id","note_datetime")) # sort seems to be an expensive operation
display(treated_notes) # sort seems to be an expensive operation

treated_notes.selectExpr("explode(note_sent) as sentences").display()
treated_notes.display()

#sentences=pipeline.fit(notes).transform(notes)
treated_notes.selectExpr("explode(cleaned_markup) as test").show(truncate=False)

#sentences.display()#show(truncate=False)

# COMMAND ----------



# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
treated_notes.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_presentation_sentences_v2")

# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
treated_notes = spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_sentences_v2")

# COMMAND ----------

import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu



@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg = (
    treated_notes.select("*", ner_udf(treated_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg)


# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
output_agg_avg.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_presentation_nerall_entities_v2")
