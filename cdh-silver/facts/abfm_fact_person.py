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
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_abfm_phi")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_abfm_phi_ra")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")


# COMMAND ----------

#task level parameters
#prefix for dictionary, name for file
#dbutils.widgets.text("datagroup",defaultValue="phi")
#key to the dictionary
##dbutils.widgets.text("conceptset",defaultValue="medication")
#dbutils.widgets.text("taskname",defaultValue="this_conceptset")

# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------

#DATAGROUP=dbutils.widgets.get("datagroup")
EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------


import shared.etl_utils as utils

# COMMAND ----------

# MAGIC %md
# MAGIC import json
# MAGIC
# MAGIC with open('./vocab_rules.json', 'r') as file:
# MAGIC     vocab = json.load(file)
# MAGIC with open('./premier_mapping.json', 'r') as file:
# MAGIC     FACT_MAPPING = json.load(file)
# MAGIC

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
"""
                                                    
vocab={
    "ICD": ["ICD-9-CM", "ICD-9", "ICD9CM",  "ICD9", "9", "09","ICD10CM", "ICD-10", "ICD9", "ICD-10-CM", "10", "1", "2", "2.16.840.1.113883.6.90",
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
#vocab['ICD']=vocab['ICD9CM']+vocab['ICD10CM']
vocab['FULL']=vocab['ICD']+vocab['HCPCS']+vocab['LOINC']+vocab['SNOMED']+vocab['CVX']+vocab['RXNORM']

icd_map={ k:'ICD' for k in vocab["ICD"]}
#icd10_map={ k:'ICD10CM' for k in vocab["ICD10CM"]}
#icd9_map={ k:'ICD9CM' for k in vocab["ICD9CM"]}
hcpcs_map={ k:'HCPCS' for k in vocab["HCPCS"]}
loinc_map={ k:'LOINC' for k in vocab["LOINC"]}
snomed_map={ k:'SNOMED' for k in vocab["SNOMED"]}
cvx_map={ k:'CVX' for k in vocab["CVX"]}
rxnorm_map={ k:'RXNORM' for k in vocab["RXNORM"]}
#icd_map = icd9_map | icd10_map
full_map=icd_map |hcpcs_map | loinc_map | snomed_map | cvx_map
print(full_map)

# Convert each item of dictionary to map type 
vocab_cast = F.create_map([F.lit(x) for x in chain(*full_map.items())])

# COMMAND ----------


FACT_MAPPING = {
    "abfm_phi_planofcare": {
        "dataset": "patientplanofcare",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "'SNOMED'",
        "code": "planofcarecode",
        "where_clause": "  ",
    },
    "abfm_phi_problem_snomed": {
        "dataset": "patientproblem",
        "person_id": "patientuid",
        "visit_start_date": "documentationdate",
        "vocabulary_id": "'SNOMED'",
        "code": "problemcode",
        "where_clause": f" and problemcategory IN {utils.unpack(vocab['SNOMED'])} ",
    },
    "abfm_phi_problem_icd": {
        "dataset": "patientproblem",
        "person_id": "patientuid",
        "visit_start_date": "documentationdate",
        "vocabulary_id": "'ICD'",
        "code": "problemcode",
        "where_clause": f" and problemcategory IN {utils.unpack(vocab['ICD'])} ",
    },

    "abfm_phi_problem": {
        "dataset": "patientproblem",
        "person_id": "patientuid",
        "visit_start_date": "documentationdate",
        "vocabulary_id": "'ICD'",
        "code": "problemcode",
        "where_clause": f" and problemcategory NOT IN {utils.unpack(vocab['SNOMED']+vocab['ICD'])} ",
    },
    "abfm_phi_immunization": {
        "dataset": "patientimmunization",
        "person_id": "patientuid",
        "visit_start_date": "immunizationstartdate",
        "vocabulary_id": "'CVX'",
        "code": "immunizationcode",
        "where_clause": f" and drugsource IN {utils.unpack(vocab['FULL'])} ",
    },
    "abfm_phi_medication_coded": {
        "dataset": "patientmedication",
        "person_id": "patientuid",
        "visit_start_date": "startdate",
        "vocabulary_id": "'DRUG'",
        "code": "medicationcode",
        "where_clause": f" and drugsource NOT IN {utils.unpack(vocab['FULL'])} ",
    },
       "abfm_phi_medication": {
        "dataset": "patientmedication",
        "person_id": "patientuid",
        "visit_start_date": "startdate",
        "vocabulary_id": "drugsource",
        "code": "medicationcode",
        "where_clause": f" and drugsource IN {utils.unpack(vocab['FULL'])} ",
    },
    "abfm_phi_procedure": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "codesystem",
        "code": "procedurecode",
        "where_clause": f" and codesystem NOT IN {utils.unpack(vocab['HCPCS'])} ",
    },
    "abfm_phi_hcpcs": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "'HCPCS'",
        "code": "procedurecode",
        "where_clause": f" and  codesystem in {utils.unpack(vocab['HCPCS'])} ",
    },
    "abfm_phi_procedure_vaccine": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "'CVX'",
        "code": "procedurecode",
        "where_clause": f" and  codesystem in {utils.unpack(vocab['CVX'])} ",
    },
    "abfm_phi_procedure_loinc": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "'LOINC'",
        "code": "procedurecode",
        "where_clause": f" and  codesystem in {utils.unpack(vocab['LOINC'])} ",
    },
    
    "abfm_phi_snomed": {
        "dataset": "patientprocedure",
        "person_id": "patientuid",
        "visit_start_date": "effectivedate",
        "vocabulary_id": "'SNOMED'",
        "code": "procedurecode",
        "where_clause": f" and codesystem in {utils.unpack(vocab['SNOMED'])}  ",
    }, 
   "abfm_phi_diagnosis": {
        "dataset": "visitdiagnosis",
        "person_id": "patientuid",
        "visit_start_date": "encounterdate",
        "vocabulary_id": "'ICD'",
        "code": "encounterdiagnosiscode",
        "where_clause": " ",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC FACT_MAPPING will eventually be a json file imported as dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC FACT_MAPPING = {
# MAGIC     "abfm_phi_planofcare": {
# MAGIC         "dataset": "patientplanofcare",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "effectivedate",
# MAGIC         "vocabulary_id": "'SNOMED'",
# MAGIC         "code": "planofcarecode",
# MAGIC         "where_clause": "  ",
# MAGIC     },
# MAGIC     "abfm_phi_problem_null": {
# MAGIC         "dataset": "patientproblem",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "documentationdate",
# MAGIC         "vocabulary_id": "'ICD'",
# MAGIC         "code": "problemcode",
# MAGIC         "where_clause": f" and problemcategory NOT IN {utils.unpack(FULL)} ",
# MAGIC     },
# MAGIC     "abfm_phi_immunization": {
# MAGIC         "dataset": "patientimmunization",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "immunizationstartdate",
# MAGIC         "vocabulary_id": "'CVX'",
# MAGIC         "code": "immunizationcode",
# MAGIC         "where_clause": " ",
# MAGIC     },
# MAGIC     "abfm_phi_medication": {
# MAGIC         "dataset": "patientmedication",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "startdate",
# MAGIC         "vocabulary_id": "drugsource",
# MAGIC         "code": "medicationcode",
# MAGIC         "where_clause": "  ",
# MAGIC     },
# MAGIC     "abfm_phi_procedure": {
# MAGIC         "dataset": "patientprocedure",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "effectivedate",
# MAGIC         "vocabulary_id": "codesystem",
# MAGIC         "code": "procedurecode",
# MAGIC         "where_clause": f" and codesystem NOT IN {utils.unpack(FULL)} ",
# MAGIC     },
# MAGIC    "abfm_phi_diagnosis": {
# MAGIC         "dataset": "visitdiagnosis",
# MAGIC         "person_id": "patientuid",
# MAGIC         "visit_start_date": "encounterdate",
# MAGIC         "vocabulary_id": "'ICD'",
# MAGIC         "code": "encounterdiagnosiscode",
# MAGIC         "where_clause": " ",
# MAGIC     },
# MAGIC      "abfm_phi_visit": {
# MAGIC          "dataset": "visit",
# MAGIC         "person_id": "patientuid",
# MAGIC          "visit_start_date": "encounterstartdate",
# MAGIC          "vocabulary_id": "'ICD'",
# MAGIC          "code": "encountertypecode",
# MAGIC          "where_clause": " ",
# MAGIC      },
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table ${DEST_CATALOG}.${DEST_SCHEMA}.fact_abfm_${DATAGROUP}_${CONCEPT} as
# MAGIC SELECT distinct ${MAPPING} as person_id,
# MAGIC startdate as visit_start_date,
# MAGIC drugsource as vocabulary_id,
# MAGIC medicationcode as code
# MAGIC from ${src.schema}.patientmedication
# MAGIC order by person_id,visit_start_date,code

# COMMAND ----------

fact_df=f"{DBCATALOG}.{DEST_SCHEMA}.fact_person"
print(fact_df)

tables=list(FACT_MAPPING.keys())
print(tables)

# COMMAND ----------

# create empty dataframe?
from pyspark.sql.types import StructType,StructField, StringType, TimestampType
schema = StructType([
  StructField('person_id', StringType(), True),
  StructField('visit_start_date', TimestampType(), True),
  StructField('src_vocabulary_id', StringType(), True),
  StructField('code', StringType(), True),
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
    print(MAPPING)

    DATASET=MAPPING['dataset']
    src_df=f"{DBCATALOG}.{SCHEMA}.{DATASET}"
    print(src_df)

    PERSON_ID=MAPPING['person_id']
    OBS_DATETIME=MAPPING['visit_start_date']
    VOCABULARY_ID=MAPPING['vocabulary_id']

    code=MAPPING['code']
    #WHERE=MAPPING["where_clause"]
    #if (first):
    #    tblstring=f"create or replace table {fact_df} as " 
    #    first=False
    #else:
    #    tblstring=f" insert into {fact_df} (person_id, visit_start_date, vocabulary_id, code, concept_source) " 

    sqlstring=f"SELECT distinct {PERSON_ID} as person_id," \
    f"{OBS_DATETIME} as visit_start_date," \
    f"UPPER({VOCABULARY_ID}) as src_vocabulary_id, " \
    f"{code} as code, " \
    f" '{TABLE}' as concept_source " \
    f"from {src_df} " \
    f"where {code} is not NULL " \
    #"and {VOCABULARY_ID} is not NULL " \
    #  f"{WHERE}" \
    f"order by person_id,visit_start_date,code"
    print(sqlstring)
# refactor to pyspark
    #colnames=["person_id","visit_start_date","vocabulary_id","code"]
    #col_list=[f"{PERSON_ID}",f"{OBS_DATETIME}",f"{VOCABULARY_ID}",f"{code}",]
    #print(col_list)
##    src_df=spark.table(f"{src_df}").select(*col_list) #.toDF(*colnames).withColumn("concept_source",f"{TABLE}")
#F.coalesce(mapping_expr[F.col("id")], F.lit("x"))
    src_df=spark.sql(sqlstring).withColumn("vocabulary_id", F.coalesce(vocab_cast[F.col("src_vocabulary_id")],F.col("src_vocabulary_id")))
    #display(src_df)
    df=df.union(src_df)
    #output=spark.sql(sqlstring)
    #display(output)
    #output=spark.sql(f"OPTIMIZE {fact_df} ZORDER BY (vocabulary_id)")
    #df=spark.table(f"{fact_df}")
    #display(df) 

# COMMAND ----------

concept_keep = [
    "code",
#    "cdh_vocabulary_id",
    "domain_id",
    "omop_vocabulary_id",
    "concept_class_id",
]
concept_join = concept_keep[0]
print(concept_join)
concept_df = (
    spark.table("edav_prd_cdh.cdh_ml.concept").withColumnRenamed('vocabulary_id','omop_vocabulary_id')
#   .withColumn(
#        "cdh_vocabulary_id",
#        F.when(
#           F.col("omop_vocabulary_id").startswith("ICD"),
#            F.lit("ICD")
#        ).otherwise(F.col("omop_vocabulary_id")),
#    )
    .select(*concept_keep)
)
display(concept_df)


# COMMAND ----------

keepcols=['person_id','visit_start_date','vocabulary_id','code','concept_source','domain_id','concept_class_id']
df=df.join(concept_df,concept_join,'left').withColumn('vocabulary_id',F.coalesce(F.col("omop_vocabulary_id"),F.col("src_vocabulary_id")))
display(df)
df.select(*keepcols).write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("vocabulary_id").saveAsTable(f"{fact_df}")
#display(df)
#df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("vocabulary_id").saveAsTable(f"{fact_df}")

# COMMAND ----------

sqlstring_p=f"ALTER TABLE {fact_df} ALTER COLUMN person_id SET NOT NULL;"
spark.sql(sqlstring_p)
sqlstring_d=f"ALTER TABLE {fact_df} ALTER COLUMN visit_start_date SET NOT NULL;"
spark.sql(sqlstring_d)
sqlstring_k=f"ALTER TABLE {fact_df} ADD CONSTRAINT abfm_person_date PRIMARY KEY(person_id, visit_start_date  TIMESERIES)"
spark.sql(sqlstring_k)


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
# MAGIC   primary_keys=["person_id", "visit_start_date"],
# MAGIC   timeseries_columns="visit_start_date",
# MAGIC   df=df,
# MAGIC )
