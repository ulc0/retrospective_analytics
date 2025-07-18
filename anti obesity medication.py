# Databricks notebook source
import pandas as pd

from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, size, collect_list, collect_set, size, monotonically_increasing_id, countDistinct, floor, datediff, to_date, lit, date_format, round
from pyspark.sql.types import DateType, StringType, StructField, StructType
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import numpy as np
spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

concept = spark.table("cdh_truveta.concept")

# COMMAND ----------

# MAGIC %md
# MAGIC # FDA-approved obesity medications (not mutually exclusive) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Wegovy

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%wegovy%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%wegovy%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/Wegovy.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Saxenda

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%saxenda%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%saxenda%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/saxenda.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Phentermine 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%phentermine%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%phentermine%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/phentermine.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Phentermine-topiramate

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%phentermine-topiramate%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%qsymia%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%qsymia%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/phentermine-topiramate.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%acxion%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%adipex-p%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%duromine%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%elvenir%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%fastin%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%fastin%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%lonamin%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%lomaira%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%panbesy%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%razin%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%redusa%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%sentis%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%suprenza%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%terfamex%')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Orlistat

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%orlistat%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%orlistat%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/orlistat.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setmelanotide 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%setmelanotide%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%setmelanotide%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/setmelanotide.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%eg imcivree%')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Non-FDA approved obesity medications 

# COMMAND ----------

# MAGIC %md
# MAGIC # Ozempic® (semaglutide) 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%ozempic%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%ozempic%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/ozempic.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Victoza

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%victoza%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%victoza%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/victoza.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Metformin 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%metformin%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%metformin%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/metformin.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Topiramate 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%topiramate%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%topiramate%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/topiramate.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%topiragen%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%topirgan%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%topamax%')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lisdexamfetamine 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%lisdexamfetamine%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%lisdexamfetamine%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/lisdexamfetamine.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%vyvanse%')).display()

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%elvanse%')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dulaglutide

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%dulaglutide%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%dulaglutide%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/dulaglutide.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%trulicity%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%trulicity%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/dulaglutide(trulicity).csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exenatide 

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%exenatide%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%exenatide%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/exenatide.csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%byetta%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%byetta%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/exenatide(byetta).csv', index=False)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%bydureon%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%bydureon%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/exenatide(bydureon).csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mounjaro (tirzepatide)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%mounjaro%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%mounjaro%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/mounjaro.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Zepbound (tirzepatide)

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%zepbound%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%zepbound%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/zepbound.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rybelsus

# COMMAND ----------

concept.filter(F.lower(concept.ConceptName).like('%rybelsus%')).display()

# COMMAND ----------

concept_pd=concept.filter(F.lower(concept.ConceptName).like('%rybelsus%')).toPandas()

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']]

# COMMAND ----------

concept_pd[['ConceptCode', 'ConceptDefinition', 'CodeSystem']].to_csv('/Workspace/Users/zsw6@cdc.gov/rybelsus.csv', index=False)