# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/parameter-value-references

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#job/run level parameters
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_truveta")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_ml")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")



# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------

EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------


import shared.etl_utils as utils


# COMMAND ----------

import pyspark.sql.functions as F
from itertools import chain 

# COMMAND ----------

#SourceProvenanceConceptId
#condition, procedure, deviceuse,extract,immunization,labresult,
# not needed medicationadministration,medicationrequest
# ,observation,
#Condition​,Procedure​,Medications​,Lab Results​,Immunization
#deviceuse stands out
FACT_MAPPING = {
    "condition": {
        "dataset": "condition",
        "person_id": "person_id",
        "observation_period_id": "EncounterId",
        "observation_datetime": "RecordedDateTime",
        "source_concept_id": "SourceProvenanceConceptId",
        "concept_id": "CategoryConceptId",
        "where_clause": "  ",
    },
    # same as condition
    "procedure": {
        "dataset": "procedure",
        "person_id": "person_id",
        "observation_period_id": "EncounterId",
        "observation_datetime": "RecordedDateTime",
        "source_concept_id": "SourceProvenanceConceptId",
        "concept_id": "CategoryConceptId",
        "where_clause": "  ",
    },
    "observation": {
        "dataset": "observation",
        "person_id": "person_id",
        "observation_period_id": "EncounterId",
        "observation_datetime": "RecordedDateTime",
        "source_concept_id": "CategoryConceptId",
        "concept_id": "CodeConceptId",
        "where_clause": "  ",
    },
}

# COMMAND ----------

fact_df=f"{DBCATALOG}.{DEST_SCHEMA}.fact_person"
print(fact_df)

tables=list(FACT_MAPPING.keys())
print(tables)

enc_df=f"{DBCATALOG}.{SCHEMA}.admissions"

# COMMAND ----------

# create empty dataframe?
from pyspark.sql.types import StructType,StructField, StringType, TimestampType
schema = StructType([
  StructField('person_id', StringType(), True),
  StructField('observation_datetime', TimestampType(), True),
  StructField('src_vocabulary_id', StringType(), True),
  StructField('concept_code', StringType(), True),
  StructField('concept_source', StringType(), True),
  StructField('vocabulary_id', StringType(), True),
  ])

df=spark.createDataFrame([],schema)

# COMMAND ----------


#first=True

#TASKNAME=dbutils.widgets.get("taskname")
for TABLE in tables:
    #CONCEPTSET=TASKNAME.split('_')[-1]

    #CONCEPT_GROUP=f"{DATAGROUP}_{CONCEPTSET}"
    #print(CONCEPT_GROUP)

    MAPPING=FACT_MAPPING[TABLE]
#    print(MAPPING)

    DATASET=MAPPING['dataset']
    src_df=f"{DBCATALOG}.{SCHEMA}.{DATASET}"
    print(src_df)

    PERSON_ID=MAPPING['person_id']
    OBS_DATETIME=MAPPING['observation_datetime']
    VOCABULARY_ID=MAPPING['vocabulary_id']

    CONCEPT_CODE=MAPPING['concept_code']
    WHERE=MAPPING["where_clause"]
    #if (first):
    #    tblstring=f"create or replace table {fact_df} as " 
    #    first=False
    #else:
    #    tblstring=f" insert into {fact_df} (person_id, observation_datetime, vocabulary_id, concept_code, concept_source) " 
  
    sqlstring=f"SELECT distinct s.{PERSON_ID} as person_id," \
    f"{OBS_DATETIME} as observation_datetime," \
    f"UPPER({VOCABULARY_ID}) as src_vocabulary_id, " \
    f"{CONCEPT_CODE} as concept_code, " \
    f" '{TABLE}' as concept_source " \
    f"from {src_df} s " \
    f"join {enc_df} e on s.hadm_id=e.hadm_id " \
    f"where {CONCEPT_CODE} is not NULL " \
    f"and {VOCABULARY_ID} is not NULL " \
    f"{WHERE}" \
    f"order by person_id,observation_datetime,concept_code"
    print(sqlstring)
# refactor to pyspark
    #colnames=["person_id","observation_datetime","vocabulary_id","concept_code"]
    #col_list=[f"{PERSON_ID}",f"{OBS_DATETIME}",f"{VOCABULARY_ID}",f"{CONCEPT_CODE}",]
    #print(col_list)
##    src_df=spark.table(f"{src_df}").select(*col_list) #.toDF(*colnames).withColumn("concept_source",f"{TABLE}")
#F.coalesce(mapping_expr[F.col("id")], F.lit("x"))
    src_df=spark.sql(sqlstring).withColumn("vocabulary_id", F.coalesce(vocab_cast[F.col("src_vocabulary_id")],F.col("src_vocabulary_id"))) #.drop('src_vocabulary_id')
    display(src_df)
    df=df.union(src_df)
    #output=spark.sql(sqlstring)
    #display(output)
    #output=spark.sql(f"OPTIMIZE {fact_df} ZORDER BY (vocabulary_id)")
    #df=spark.table(f"{fact_df}")
    #display(df) 

# COMMAND ----------

concept_keep = [
    "concept_code",
    "cdh_vocabulary_id",
    "domain_id",
    "omop_vocabulary_id",
    "concept_class_id",
]
concept_join = concept_keep[0]
print(concept_join)
concept_df = (
    spark.table("edav_dev_cdh.cdh_ml.concept").withColumnRenamed('vocabulary_id','omop_vocabulary_id')
    .withColumn(
        "cdh_vocabulary_id",
        F.when(
            F.col("omop_vocabulary_id").startswith("ICD"),
            F.lit("ICD")
        ).otherwise(F.col("omop_vocabulary_id")),
    )
    .select(*concept_keep)
)
#display(concept_df)


# COMMAND ----------

keepcols=['person_id','observation_datetime','vocabulary_id','concept_code','concept_source','domain_id','concept_class_id']
df=df.join(concept_df,concept_join,'left').withColumn('vocabulary_id',F.coalesce(F.col("omop_vocabulary_id"),F.col("src_vocabulary_id")))
display(df)
df.select(*keepcols).write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("vocabulary_id").saveAsTable(f"{fact_df}")

# COMMAND ----------

sqlstring_p=f"ALTER TABLE {fact_df} ALTER COLUMN person_id SET NOT NULL;"
spark.sql(sqlstring_p)
sqlstring_d=f"ALTER TABLE {fact_df} ALTER COLUMN observation_datetime SET NOT NULL;"
spark.sql(sqlstring_d)
sqlstring_k=f"ALTER TABLE {fact_df} ADD CONSTRAINT abfm_person_date PRIMARY KEY(person_id, observation_datetime  TIMESERIES)"
spark.sql(sqlstring_k)


# COMMAND ----------

#https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/time-series
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
# user_features_df DataFrame contains the following columns:
# - user_id
# - ts
# - purchases_30d
# - is_free_trial_active
fe.create_table(
  name=f"{fact_df}",
  primary_keys=["person_id", "observation_datetime"],
  timeseries_columns="observation_datetime",
  df=df,
)
