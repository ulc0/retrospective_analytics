# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

#concept_set/concept_set_table.py
dbutils.widgets.text("SRC_CATALOG",defaultValue="edav_prd_cdh")
SRC_CATALOG=dbutils.widgets.get("SRC_CATALOG")
dbutils.widgets.text('SRC_SCHEMA',defaultValue='cdh_truveta_lava_dev')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text("CATALOG",defaultValue="edav_prd_cdh")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text('SCHEMA',defaultValue='cdh_ml')
SCHEMA=dbutils.widgets.get('SCHEMA')


# COMMAND ----------

concept_set=f"{SRC_CATALOG}.{SRC_SCHEMA}"
tablesObj=spark.catalog.listTables(concept_set)


# COMMAND ----------

tables=[t[0] for t in tablesObj if t[0].startswith('code_') and ('rxnorm' in t[0] or 'icd' in t[0] or 'loinc' in [0]) and 'idd' not in t[0]]
print(tables)

# COMMAND ----------


schema = StructType([ 
	StructField('code', 
				StringType(), False), 
	StructField('vocabulary_id', 
				StringType(), False), 
	StructField('feature_name', 
				StringType(), True), 
]) 


# COMMAND ----------

vocab_map={'icd':'ICD10' ,'icdcodes':'ICD10','ndc':'RXNORM','loinc':'LOINC'}

# COMMAND ----------

df=spark.createDataFrame([],schema)
for t in tables:
    codes=spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.{t}")
    featurecol=codes.columns[3]
    conceptcols=codes.columns[5]
    parts=conceptcols.split('_')[:-1]
    print(parts)
    vocab=vocab_map[parts[1]]
    sqlstring=f"select {conceptcols} as code, '{vocab}' as vocabulary_id,valueset as feature_name  from  {SRC_CATALOG}.{SRC_SCHEMA}.{t}"
    print(sqlstring)
    src_df=spark.sql(sqlstring)
    df=df.union(src_df)


# COMMAND ----------

ft_name=f"{CATALOG}.{SCHEMA}.concept_sets"
write_mode = "append"
schema_option = "mergeSchema"

df.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable( ft_name)
