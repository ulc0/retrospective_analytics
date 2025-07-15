# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC import pyspark.sql.functions as F
# MAGIC rest_cols = [F.col(cl) for cl in df.columns if cl not in mapping]
# MAGIC conv_cols = [F.col(cl_name).cast(cl_type).alias(cl_name) 
# MAGIC    for cl_name, cl_type in mapping.items()
# MAGIC    if cl_name in df.columns]
# MAGIC conv_df.select(*rest_cols, *conv_cols)

# COMMAND ----------

src_schema='dev_cdh_ml'
src_catalog='edav_prd_cdh'
src_prefix='raw_synthea'
#/Volumes/edav_prd_cdh/cdh_ml/metadata_data/mitre_synthea10k/
src_catalog='/Volumes/edav_prd_cdh'
src_schema='/cdh_ml'
src_prefix='/metadata_data/mitre_synthea10k'
tlist=['allergies', 
'careplans',
#'claims',
#'claims_transactions',
'conditions',
'devices',
'encounters',
'imaging_studies',
'immunizations',
'medications',
'observations',
#'organizations', 
'patients',
#'payer_transitions',
#'payers',
'procedures',
'providers',
'supplies',]



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, TimestampType

schema = StructType([ 
	StructField('visit_start_date', 
				TimestampType(), True), 
	StructField('person_id', 
				StringType(), False), 
	StructField('code', 
				StringType(), True), 
	StructField('src_vocabulary_id', 
				StringType(), True), 
	StructField('src_table', 
				StringType(), True), 
# added during join with concept table
#	StructField('source_concept_id', 
#				LongType(), True), 
#	StructField('domain_id', 
#				LongType(), True), 
]) 
fact_df=spark.createDataFrame([],schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC df3 = df2.selectExpr("cast(age as int) age",  
# MAGIC     "cast(isGraduated as string) isGraduated",  
# MAGIC         "cast(jobStartDate as string) jobStartDate")  
# MAGIC df3.printSchema()  
# MAGIC df3.show(truncate=False)  
# MAGIC
# MAGIC df3.createOrReplaceTempView("CastExample")  
# MAGIC df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated),DATE(jobStartDate) from CastExample")  
# MAGIC df4.printSchema()
# MAGIC ```   

# COMMAND ----------

import json
#with open("synthea_process.json") as f:
#    castdict = json.load(f)
castdict={
    "PATIENT": "patient_id",
    "ENCOUNTER":"visit_occurrence_number",
    "START": "visit_start_date",
    "DATE": "visit_start_date",
    "CODE": "code",
    "BODYSITE_CODE": "code",
    "PROCEDURECODE": "code",
    "DIAGNOSIS1": "code",
    "SYSTEM": "vocabulary_id",
}
clist=list(castdict.keys())
print(clist)

vocabdict={"observations":"LOINC",
           "immunizations":"CVX",
           "medications":"RXNORM",}



# COMMAND ----------

for tbl in tlist:
    print(tbl)
#    df=spark.table(f"{src_catalog}.{src_schema}.{src_prefix}_{tbl}")
    fname=f"{src_catalog}/{src_schema}/{src_prefix}/{tbl}.csv"
#    display(fname)
    df=spark.read.options(header=True,).csv(fname)
    #display(df)
    dfcols=list(df.columns)
    #print(df.columns)
 #   print(dfcols)
 #   print(clist)
 #   keep=list(set(clist)&set(dfcols))
    keep=[x for x in dfcols if x in clist]
    if len(keep) >0: #== len(clist):
        print(keep)
        vocab="SNOMED"
        if "SYSTEM" not in keep:
            if (tbl in vocabdict.keys()):
                vocab=vocabdict[tbl]
                print(vocab)
            keep=keep+['SYSTEM']
        #print(keep)
        xdf=df.withColumn("SYSTEM",F.lit(vocab)).select(*keep).withColumnsRenamed(castdict).withColumn('source_table',F.lit(tbl)).distinct()
        #display(xdf)
        print(xdf.columns)
        fact_df=fact_df.union(xdf)
    #print(f"Writing {tbl}")
    #df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"edav_prd_cdh.cdh_ml.synthea_{tbl}")

# COMMAND ----------

display(fact_df)

# COMMAND ----------


domains=['Procedure','Condition','Drug','Observation','Specimen','Visit','Meas Value','Type Concept',]
dstr=",".join(domains)
print(dstr)
concept=spark.sql(f"select code,vocabulary_id,concept_id,domain_id from cdh_ml.concept where domain_id in ('Procedure','Condition','Drug','Observation','Specimen','Visit','Meas Value','Type Concept') order by code, vocabulary_id")
display(concept)

# COMMAND ----------

fact_tbl=fact_df.join(concept,['code'],'left')
display(fact_df)

# COMMAND ----------

#quick QC

display(fact_tbl.groupBy('src_table','domain_id','src_vocabulary_id','vocabulary_id',).count().orderBy('src_table','src_vocabulary_id'))
#display(fact_tbl.where(isnull(fact_tbl.domain_id)))

# COMMAND ----------


fact_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"edav_prd_cdh.cdh_ml.synthea_fact_person")
