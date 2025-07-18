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
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_abfm_phi")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_abfm_phi_ra")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")


# COMMAND ----------

#task level parameters
#prefix for dictionary, name for file
dbutils.widgets.text("datagroup",defaultValue="abfm_phi")
#key to the dictionary
##dbutils.widgets.text("conceptset",defaultValue="medication")
#dbutils.widgets.text("taskname",defaultValue="this_conceptset")

# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------

DATAGROUP=dbutils.widgets.get("datagroup")
EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC TS_MAPPING will eventually be a json file imported as dictionary

# COMMAND ----------

# df.replace(list(deviceDict.keys()), list(deviceDict.values()), 'device_type')
pp_category_dict = {
    '1':'ICD',
    'icd9cm':'ICD',
    '9':'ICD',
    '09':'ICD',
    'icd10cm':'ICD',
    '2':'ICD',
    '2.16.840.1.113883.6.90':'ICD',
    '10':'ICD',
    '3':'SNOMED',
    'snomedct':'SNOMED',
    '2.16.840.1.113883.6.96':'SNOMED',
}
def unpack(s):
#    return " ".join(map(str, s))  # map(), just for kicks
    return f"({s})"

dictionary = {'A': 'Apple', 'B': 'Ball', 
              'C': 'Cat', 'D': 'Dog', 
              'E': 'Elephant'} 
  
# Convert each item of dictionary to map type 
#mapping_expr = F.create_map([F.lit(x) for x in F.chain(*dictionary.items())]) 
  
# Create a new column by calling the function to map the values 
#df.withColumn("value", 
#              mapping_expr[F.col("key")]).show()


# COMMAND ----------

import pyspark.sql.functions as F
from itertools import chain 

ICD=['ICD-9-CM','ICD10CM','ICD9CM','ICD-10','ICD9','ICD-10-CM','9','09']
HCPCS=['6','CPT4','HCPCS','hcpcs-Level-II','CPT','CPT-4',]
LOINC=['4','LOINC',]        
SNOMED=['5','SNOMED','SNOMED-CT','SNOMEDCT','SNOMED CT',]
CVX=['CVX','cvx','VACCINE',]
FULL=ICD+HCPCS+LOINC+SNOMED+CVX

icd_map={ k:'ICD' for k in ICD}
hcpcs_map={ k:'HCPCS' for k in HCPCS}
loinc_map={ k:'LOINC' for k in LOINC}
snomed_map={ k:'SNOMED' for k in SNOMED}
cvx_map={ k:'CVX' for k in CVX}
full_map=icd_map | hcpcs_map | loinc_map | snomed_map | cvx_map
print(full_map)


# Convert each item of dictionary to map type 
vocab_cast = F.create_map([F.lit(x) for x in chain(*full_map.items())])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC TS_MAPPING = {
# MAGIC     "abfm_phi_planofcare": {
# MAGIC         "dataset": "patientplanofcare",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "'SNOMED'",
# MAGIC         "concept_code": "planofcarecode",
# MAGIC         "where_clause": "  ",
# MAGIC     },
# MAGIC """
# MAGIC     "abfm_phi_problem_snomed": {
# MAGIC         "dataset": "patientproblem",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "documentationdate",
# MAGIC         "vocabulary_id": "'SNOMED'",
# MAGIC         "concept_code": "problemcode",
# MAGIC         "where_clause": f" and problemcategory IN {unpack(SNOMED)} ",
# MAGIC     },
# MAGIC     "abfm_phi_problem_icd": {
# MAGIC         "dataset": "patientproblem",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "documentationdate",
# MAGIC         "vocabulary_id": "'ICD'",
# MAGIC         "concept_code": "problemcode",
# MAGIC         "where_clause": f" and problemcategory IN {unpack(ICD)} ",
# MAGIC     },
# MAGIC """
# MAGIC     "abfm_phi_problem": {
# MAGIC         "dataset": "patientproblem",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "documentationdate",
# MAGIC         "vocabulary_id": "problemcategory",
# MAGIC         "concept_code": "problemcode",
# MAGIC         "where_clause": f" and problemcategory NOT IN {unpack(FULL)} ",
# MAGIC     },
# MAGIC     "abfm_phi_immunization": {
# MAGIC         "dataset": "patientimmunization",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "immunizationstartdate",
# MAGIC         "vocabulary_id": "'CVX'",
# MAGIC         "concept_code": "immunizationcode",
# MAGIC         "where_clause": " ",
# MAGIC     },
# MAGIC     "abfm_phi_medication": {
# MAGIC         "dataset": "patientmedication",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "startdate",
# MAGIC         "vocabulary_id": "drugsource",
# MAGIC         "concept_code": "medicationcode",
# MAGIC         "where_clause": "  ",
# MAGIC     },
# MAGIC     "abfm_phi_procedure": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "codesystem",
# MAGIC         "concept_code": "procedurecode",
# MAGIC         "where_clause": f" and codesystem NOT IN {unpack(FULL)} ",
# MAGIC     },
# MAGIC """    
# MAGIC     "abfm_phi_hcpcs": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "'HCPCS'",
# MAGIC         "concept_code": "procedurecode",
# MAGIC         "where_clause": f" and  codesystem in {unpack(HCPCS)} ",
# MAGIC     },
# MAGIC     "abfm_phi_procedure_vaccine": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "'CVX'",
# MAGIC         "concept_code": "procedurecode",
# MAGIC         "where_clause": f" and  codesystem in {unpack(CVX)} ",
# MAGIC     },
# MAGIC     "abfm_phi_procedure_loinc": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "'LOINC'",
# MAGIC         "concept_code": "procedurecode",
# MAGIC         "where_clause": f" and  codesystem in {unpack(LOINC)} ",
# MAGIC     },
# MAGIC     
# MAGIC     "abfm_phi_snomed": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "effectivedate",
# MAGIC         "vocabulary_id": "'SNOMED'",
# MAGIC         "concept_code": "procedurecode",
# MAGIC         "where_clause": f" and codesystem in {unpack(SNOMED)}  ",
# MAGIC     },
# MAGIC """    
# MAGIC    "abfm_phi_diagnosis": {
# MAGIC         "dataset": "visitdiagnosis",
# MAGIC         "person_id": "patientuid",
# MAGIC         "observation_datetime": "encounterdate",
# MAGIC         "vocabulary_id": "'ICD'",
# MAGIC         "concept_code": "encounterdiagnosiscode",
# MAGIC         "where_clause": " ",
# MAGIC     },
# MAGIC }

# COMMAND ----------


TS_MAPPING = {
    "abfm_phi_planofcare": {
        "dataset": "patientplanofcare",
        "person_id": "patientuid",
        "observation_datetime": "effectivedate",
        "vocabulary_id": "''",
        "concept_code": "planofcarecode",
        "where_clause": "  ",
    },
    "abfm_phi_problem": {
        "dataset": "patientproblem",
        "person_id": "patientuid",
        "observation_datetime": "documentationdate",
        "vocabulary_id": "problemcategory",
        "concept_code": "problemcode",
        "where_clause": f" and problemcategory NOT IN {unpack(FULL)} ",
    },
    "abfm_phi_immunization": {
        "dataset": "patientimmunization",
        "person_id": "patientuid",
        "observation_datetime": "immunizationstartdate",
        "vocabulary_id": "'CVX'",
        "concept_code": "immunizationcode",
        "where_clause": " ",
    },
    "abfm_phi_medication": {
        "dataset": "patientmedication",
        "person_id": "patientuid",
        "observation_datetime": "startdate",
        "vocabulary_id": "drugsource",
        "concept_code": "medicationcode",
        "where_clause": "  ",
    },
    "abfm_phi_procedure": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "observation_datetime": "effectivedate",
        "vocabulary_id": "codesystem",
        "concept_code": "procedurecode",
        "where_clause": f" and codesystem NOT IN {unpack(FULL)} ",
    },
   "abfm_phi_diagnosis": {
        "dataset": "visitdiagnosis",
        "person_id": "patientuid",
        "observation_datetime": "encounterdate",
        "vocabulary_id": "'ICD'",
        "concept_code": "encounterdiagnosiscode",
        "where_clause": " ",
    },
     "abfm_phi_visit": {
         "dataset": "visit",
        "person_id": "patientuid",
         "observation_datetime": "encounterstartdate",
         "vocabulary_id": "'ICD'",
         "concept_code": "encountertypecode",
         "where_clause": " ",
     },
}

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table ${DEST_CATALOG}.${DEST_SCHEMA}.ts_${DATAGROUP}_${CONCEPT} as
# MAGIC SELECT distinct ${MAPPING} as person_id,
# MAGIC startdate as observation_datetime,
# MAGIC drugsource as vocabulary_id,
# MAGIC medicationcode as concept_code
# MAGIC from ${src.schema}.patientmedication
# MAGIC order by person_id,observation_datetime,concept_code

# COMMAND ----------

ts_df=f"{DBCATALOG}.{DEST_SCHEMA}.ft_timeseries"
print(ts_df)

tables=list(TS_MAPPING.keys())
print(tables)

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

    MAPPING=TS_MAPPING[TABLE]
    print(MAPPING)

    DATASET=MAPPING['dataset']
    src_df=f"{DBCATALOG}.{SCHEMA}.{DATASET}"
    print(src_df)

    PERSON_ID=MAPPING['person_id']
    OBS_DATETIME=MAPPING['observation_datetime']
    VOCABULARY_ID=MAPPING['vocabulary_id']

    CONCEPT_CODE=MAPPING['concept_code']
    #WHERE=MAPPING["where_clause"]
    #if (first):
    #    tblstring=f"create or replace table {ts_df} as " 
    #    first=False
    #else:
    #    tblstring=f" insert into {ts_df} (person_id, observation_datetime, vocabulary_id, concept_code, concept_source) " 

    sqlstring=f"SELECT distinct {PERSON_ID} as person_id," \
    f"{OBS_DATETIME} as observation_datetime," \
    f"UPPER({VOCABULARY_ID}) as src_vocabulary_id, " \
    f"{CONCEPT_CODE} as concept_code, " \
    f" '{TABLE}' as concept_source " \
    f"from {src_df} " \
    f"where {CONCEPT_CODE} is not NULL " \
    #"and {VOCABULARY_ID} is not NULL " \
  #  f"{WHERE}" \
    f"order by person_id,observation_datetime,concept_code"
    print(sqlstring)
# refactor to pyspark
    #colnames=["person_id","observation_datetime","vocabulary_id","concept_code"]
    #col_list=[f"{PERSON_ID}",f"{OBS_DATETIME}",f"{VOCABULARY_ID}",f"{CONCEPT_CODE}",]
    #print(col_list)
##    src_df=spark.table(f"{src_df}").select(*col_list) #.toDF(*colnames).withColumn("concept_source",f"{TABLE}")
#F.coalesce(mapping_expr[F.col("id")], F.lit("x"))
    src_df=spark.sql(sqlstring).withColumn("vocabulary_id", F.coalesce(vocab_cast[F.col("src_vocabulary_id")],F.col("src_vocabulary_id")))
    #display(src_df)
    df=df.union(src_df)
    #output=spark.sql(sqlstring)
    #display(output)
    #output=spark.sql(f"OPTIMIZE {ts_df} ZORDER BY (vocabulary_id)")
    #df=spark.table(f"{ts_df}")
    #display(df) 

# COMMAND ----------

display(df)
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("vocabulary_id").saveAsTable(f"{ts_df}")

# COMMAND ----------

sqlstring_p=f"ALTER TABLE {ts_df} ALTER COLUMN person_id SET NOT NULL;"
spark.sql(sqlstring_p)
sqlstring_d=f"ALTER TABLE {ts_df} ALTER COLUMN observation_datetime SET NOT NULL;"
spark.sql(sqlstring_d)
sqlstring_k=f"ALTER TABLE {ts_df} ADD CONSTRAINT abfm_person_date PRIMARY KEY(person_id, observation_datetime  TIMESERIES)"
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
  name=f"{ts_df}",
  primary_keys=["person_id", "observation_datetime"],
  timeseries_columns="observation_datetime",
  df=df,
)
