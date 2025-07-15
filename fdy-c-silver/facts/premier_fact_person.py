# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/parameter-value-references

# COMMAND ----------

# MAGIC %md
# MAGIC install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#job/run level parameters
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("DEST_CATALOG",defaultValue="edav_prd_cdh")

dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_premier_v2")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_premier_ra")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")

dbutils.widgets.text("enc_df",defaultValue="edav_prd_cdh.cdh_premier_ra.fact_encounter_index")
dbutils.widgets.text("fact_df",defaultValue="edav_prd_cdh.cdh_premier_ra.fact_person")

# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
DEST_CATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------


EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------

enc_df=dbutils.widgets.get('enc_df') #f"{DEST_CATALOG}.{DEST_SCHEMA}.fact_encounter_index"
fact_df=dbutils.widgets.get('fact_df') #f"{DEST_CATALOG}.{DEST_SCHEMA}.fact_patient"
PERSON_ID='person_id'


# COMMAND ----------


import shared.etl_utils as utils


# COMMAND ----------

# MAGIC %md
# MAGIC FACT_MAPPING will eventually be a json file imported as dictionary
# MAGIC """
# MAGIC import jsonF
# MAGIC
# MAGIC with open('vocab_rules.json', 'r') as file:
# MAGIC     vocab = json.load(file)
# MAGIC """

# COMMAND ----------

import pyspark.sql.functions as F
from itertools import chain 
"""
ICD=['ICD-9-CM','ICD-9','ICD10CM','ICD9CM','ICD-10','ICD9','ICD-10-CM','9','09','10','1','2','2.16.840.1.113883.6.90',]
HCPCS=['6','CPT4','HCPCS','hcpcs-Level-II','CPT','CPT-4',]
LOINC=['4','LOINC',]        
SNOMED=['5','SNOMED','SNOMED-CT','SNOMEDCT','SNOMED CT','3','2.16.840.1.113883.6.96',]
CVX=['CVX','cvx','VACCINE',]
RXNORM=['RxNorm','RXNORM','2.16.840.1.113883.6.88',]
FULL=ICD+HCPCS+LOINC+SNOMED+CVX+RXNORM
"""
                                                    
vocab={
    "ICD9CM": ["ICD-9-CM", "ICD-9", "ICD9CM",  "ICD9", "9", "09", 
    ],
    "ICD10CM": [ "ICD10CM", "ICD-10", "ICD9", "ICD-10-CM", "10", "1", "2", "2.16.840.1.113883.6.90",
    ],
    "HCPCS": ["6", "CPT4", "HCPCS", "hcpcs-Level-II", "CPT", "CPT-4",
    ],
    "LOINC": ["4", "LOINC",
    ],
    "SNOMED": ["5", "SNOMED", "SNOMED-CT", "SNOMEDCT", "SNOMED CT", "3", "2.16.840.1.113883.6.96",
    ],
    "CVX": ["CVX", "cvx", "VACCINE",
    ],
    "RXNORM": ["RxNorm", "RXNORM", "2.16.840.1.113883.6.88",
    ],
}
vocab['ICD']=vocab['ICD9CM']+vocab['ICD10CM']

icd10_map={ k:'ICD10CM' for k in vocab["ICD10CM"]}
icd9_map={ k:'ICD9CM' for k in vocab["ICD9CM"]}
hcpcs_map={ k:'HCPCS' for k in vocab["HCPCS"]}
loinc_map={ k:'LOINC' for k in vocab["LOINC"]}
snomed_map={ k:'SNOMED' for k in vocab["SNOMED"]}
cvx_map={ k:'CVX' for k in vocab["CVX"]}
rxnorm_map={ k:'RXNORM' for k in vocab["RXNORM"]}
icd_map = icd9_map | icd10_map
full_map=icd9_map |icd10_map | hcpcs_map | loinc_map | snomed_map | cvx_map
print(full_map)

# Convert each item of dictionary to map type 
vocab_cast = F.create_map([F.lit(x) for x in chain(*full_map.items())])

# COMMAND ----------

# A 42894222 P 44786627 S 44786629
#                "paticd_proc_P:{"P": "44786630", "S": "4478663"},

TYPE_CONCEPT_MAP={"paticd_diag_A":"42894222",
                  "paticd_diag_P":"44786627",
                  "paticd_diag_S":"44786629",  
                "paticd_proc_P": "44786630",
                 "paticd_proc_S": "4478663",
                "patcpt":"38000275",
                "genlab":"44818702",
                "vitals":"44818702",
                "lab_sens":"44818702",
                "stdlab":"44818702",
                "stdrx":"32869",
                "stdchg":"32853",
                }

# COMMAND ----------




# this can be a dictionary


FACT_MAPPING = {
    "patcpt": {
        "dataset": "patcpt",
        # "observation_period_number": "PAT_KEY",
        #        "person_id": "patientuid",
        "observation_datetime": "coalesce(PROC_DATE,e.observation_datetime)",
        "vocabulary_id": "'HCPCS'",
        "concept_code": "CPT_CODE",
#        "type_concept": "TYPE_CONCEPT",
        "where_clause": "  ",
    },
    "paticd_diag_A": {
        "dataset": "paticd_diag",
        #  "observation_period_number": "PAT_KEY",
        #       "person_id": "patientuid",
        "observation_datetime": "e.observation_datetime ",
        "vocabulary_id": "ICD_VERSION",
        "concept_code": "ICD_CODE",
        "type_concept": "A",
        "where_clause": " WHERE e.observation_datetime is NOT NULL AND (ICD_PRI_SEC = 'A' OR ICD_POA='Y') ",
    },
    "paticd_diag_P": {
        "dataset": "paticd_diag",
        #  "observation_period_number": "PAT_KEY",
        #       "person_id": "patientuid",
        "observation_datetime": " coalesce(e.observation_period_end_date,e.observation_datetime)   ",
        "vocabulary_id": "ICD_VERSION",
        "concept_code": "ICD_CODE",
        "type_concept": "paticd_diag,'ICD_PRI_SEC'",
        "where_clause": " WHERE e.observation_datetime is NOT NULL AND (ICD_PRI_SEC ='P' AND ICD_POA!='Y') ",
    },
    "paticd_diag_S": {
        "dataset": "paticd_diag",
        #  "observation_period_number": "PAT_KEY",
        #       "person_id": "patientuid",
        "observation_datetime": " coalesce(e.observation_period_end_date,e.observation_datetime)   ",
        "vocabulary_id": "ICD_VERSION",
        "concept_code": "ICD_CODE",
#        "type_concept": "paticd_diag,'ICD_PRI_SEC'",
        "where_clause": " WHERE e.observation_datetime is NOT NULL AND (ICD_PRI_SEC = 'S' AND ICD_POA!='Y') ",
    },
    "paticd_proc_P": {
        "dataset": "paticd_proc",
        #   "observation_period_number": "PAT_KEY",
        #        "person_id": "patientuid",
        "observation_datetime": " coalesce(PROC_DATE,e.observation_datetime) ",
        "vocabulary_id": "'HCPCS'",
        "type_concept": "paticd_proc,'ICD_PRI_SEC'",
        "concept_code": "ICD_CODE",
        "where_clause": " where ICD_PRI_SEC='P' ",
    },
    "paticd_proc_S": {
        "dataset": "paticd_proc",
        #   "observation_period_number": "PAT_KEY",
        #        "person_id": "patientuid",
        "observation_datetime": " coalesce(PROC_DATE,e.observation_datetime) ",
        "vocabulary_id": "'HCPCS'",
        "type_concept": "paticd_proc,'ICD_PRI_SEC'",
        "concept_code": "ICD_CODE",
        "where_clause": " where ICD_PRI_SEC='S' ",
    },
    "genlab": {
        "dataset": "genlab",
        # "observation_period_number": "PAT_KEY",
        #  "person_id": "patientuid",
        "observation_datetime": "collection_datetime",
        "vocabulary_id": "LAB_TEST_CODE_TYPE",
        "concept_code": "lab_test_code",
#        "type_concept": "TYPE_CONCEPT",
        "where_clause": " WHERE  collection_datetime is not NULL ",
    },
    "lab_sens": {
        "dataset": "lab_sens",
        # "observation_period_number": "PAT_KEY",
        # "person_id": "patientuid",
        "observation_datetime": "collection_datetime",
        "vocabulary_id": "SUSC_TEST_METHOD_CODE_TYPE",
 #       "type_concept": "TYPE_CONCEPT",
        "concept_code": "SUSC_TEST_METHOD_CODE",
        "where_clause": " WHERE  collection_datetime is not NULL ",
    },
    "vitals": {
        "dataset": "vitals",
        # "observation_period_number": "PAT_KEY",
        # "person_id": "patientuid",
        "observation_datetime": "e.observation_datetime",
        "vocabulary_id": "LAB_TEST_CODE_TYPE",
        "concept_code": "LAB_TEST_CODE",
  #      "type_concept": "TYPE_CONCEPT",
        "where_clause": " WHERE  e.observation_datetime is not NULL ",
    },
    "stdrx": {
        "dataset": "patbill",
        # "observation_period_number": "PAT_KEY",
        # "person_id": "patientuid",
        "observation_datetime": "COALESCE(serv_date,e.observation_datetime)",
        "vocabulary_id": "'STDRX'",
        "concept_code": "std_chg_code",
   #     "type_concept": "TYPE_CONCEPT",
        "where_clause": " WHERE  startswith(std_chg_code,'250') ",
    },
    "stdlab": {
        "dataset": "patbill",
        # "observation_period_number": "PAT_KEY",
        # "person_id": "patientuid",
        "observation_datetime": "COALESCE(serv_date,e.observation_datetime)",
        "vocabulary_id": "'STDLAB'",
        "concept_code": "std_chg_code",
    #    "type_concept": "TYPE_CONCEPT",
        "where_clause": " WHERE  startswith(std_chg_code,'300') ",
    },
    "stdchg": {
        "dataset": "patbill",
        # "observation_period_number": "PAT_KEY",
        # "person_id": "patientuid",
        "observation_datetime": "COALESCE(serv_date,e.observation_datetime)",
        "vocabulary_id": "'STDCHG'",
        "concept_code": "std_chg_code",
#        "type_concept": "TYPE_CONCEPT",
        "where_clause": " WHERE NOT startswith(std_chg_code,'250') AND NOT startswith(std_chg_code,'300') ",
    },
}

#with open('premier_mapping.json', 'r') as file:
#    FACT_MAPPING = json.load(file)


# COMMAND ----------

tables=list(FACT_MAPPING.keys())
print(tables)

# COMMAND ----------

# create empty dataframe
from pyspark.sql.types import StructType,StructField, StringType, TimestampType, IntegerType

schema = StructType([
  StructField('person_id', IntegerType(), True),
  StructField('observation_datetime', TimestampType(), True),
  StructField('observation_period_number', IntegerType(), True),
  StructField('src_vocabulary_id', StringType(), True),
  StructField('concept_code', StringType(), True),
  StructField('type_concept', StringType(), True),
  StructField('vocabulary_id', StringType(), True),
  ])
df=spark.createDataFrame([],schema)
print(df.columns)
colnames=list(df.columns)
print(colnames)

# COMMAND ----------

for TABLE in tables:
    MAPPING=FACT_MAPPING[TABLE]
    #print(MAPPING)
    print(TABLE)
    #observation_period_number=MAPPING['observation_period_number']
    OBS_DATETIME=MAPPING['observation_datetime']
    VOCABULARY_ID=MAPPING['vocabulary_id']
    CONCEPT_CODE=MAPPING['concept_code']
    DATASET=MAPPING["dataset"]
    WHERE=MAPPING['where_clause']
    TYPE_CONCEPT=TYPE_CONCEPT_MAP[TABLE]

    print(TYPE_CONCEPT)
    src_df=f"{DBCATALOG}.{SCHEMA}.{DATASET}"

    sqlstring=f"SELECT distinct {PERSON_ID} as person_id," \
    f"{OBS_DATETIME} as observation_datetime," \
    f"observation_period_number," \
    f"UPPER({VOCABULARY_ID}) as src_vocabulary_id, " \
    f"{CONCEPT_CODE} as concept_code, " \
    f"'{TYPE_CONCEPT}' as type_concept " \
    f"from {src_df} s " \
    f"join {enc_df} e on s.PAT_KEY=e.observation_period_number " \
    f"{WHERE}  " 
#    f"pat_key as observation_period_number," \
    #f"where {CONCEPT_CODE} is not NULL " \
    #f"and {VOCABULARY_ID} is not NULL " \
    #f"and {observation_period_number} is not NULL " \
    #f"order by person_id,observation_datetime,observation_period_number,concept_code"
    print(sqlstring)
    src_df=spark.sql(sqlstring).withColumn("vocabulary_id", F.coalesce(vocab_cast[F.col("src_vocabulary_id")],F.col("src_vocabulary_id")))
        #display(src_df)
    df=df.union(src_df.select(*colnames))
#    f"'{DATASET}' as concept_source " \

# COMMAND ----------

"""
concept=spark.sql(f"select concept_code,vocabulary_id,concept_id,domain_id from cdh_ml.concept where domain_id in ('Procedure','Condition','Drug','Observation','Specimen','Visit','Meas Value','Type Concept') order by concept_code, vocabulary_id")
display(concept)
"""


# COMMAND ----------
"""

domains=['Procedure','Condition','Drug','Observation','Specimen','Visit','Meas Value','Type Concept',]
concept_keep=['concept_code','vocabulary_id','domain_id','concept_class_id',]
concept_join=concept_keep[:2]
print(concept_join)
concept_df=spark.table(f"{DBCATALOG}.cdh_ml.concept").where(F.col('domain_id').isin(*domains)).select(*concept_keep)
display(concept_df)
df=df.join(concept_df,concept_join,'left')
"""

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{fact_df}") #.partitionBy("vocabulary_id").saveAsTable(f"{fact_df}")

# COMMAND ----------

# MAGIC %md
# MAGIC sqlstring_p=f"ALTER TABLE {fact_df} ALTER COLUMN person_id SET NOT NULL;"
# MAGIC spark.sql(sqlstring_p)
# MAGIC sqlstring_d=f"ALTER TABLE {fact_df} ALTER COLUMN observation_datetime SET NOT NULL;"
# MAGIC spark.sql(sqlstring_d)
# MAGIC sqlstring_k=f"ALTER TABLE {fact_df} ADD CONSTRAINT premier_person_date PRIMARY KEY(person_id, observation_datetime  TIMESERIES)"
# MAGIC spark.sql(sqlstring_k)
# MAGIC

# COMMAND ----------

print(fact_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/time-series
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC fe = FeatureEngineeringClient()
# MAGIC # user_features_df DataFrame contains the following columns:
# MAGIC # - user_id
# MAGIC # - ts
# MAGIC # - purchases_30d
# MAGIC # - is_free_trial_active
# MAGIC fe.create_table(
# MAGIC   name=f"{fact_df}",
# MAGIC   primary_keys=["person_id", "observation_datetime"],
# MAGIC   timeseries_columns="observation_datetime",
# MAGIC   df=df,
# MAGIC )
