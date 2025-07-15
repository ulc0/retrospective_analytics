# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Create Features by Text Search of files
# MAGIC
# MAGIC patlabres=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.lab_res")  
# MAGIC genlab=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.genlab")  
# MAGIC
# MAGIC disstat=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.disstat")  
# MAGIC
# MAGIC pattype=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.pattype")  
# MAGIC providers=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.providers")  
# MAGIC zip3=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.phd_providers_zip3")  
# MAGIC prov=providers.join(zip3,"prov_id","inner")  
# MAGIC payor = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.payor")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC readmit=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.readmit")  
# MAGIC patdemo=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.patdemo")  
# MAGIC

# COMMAND ----------


def listFilter(tblName,tblCol,tblCode,tblDesc,regex):
    codesql=f"select {tblCol} as concept_code,'{tblCode}' as vocabulary_id from {tblName} where {tblDesc} RLIKE '{regex}' "
    print(codesql)
    testfile=spark.sql(codesql)
    return testfile

# COMMAND ----------

# MAGIC %md
# MAGIC #ccsr_lookup = spark.table("edav_prd_cdh.cdh_sandbox.icd10cm_diagnosis_codes_to_ccsr_categories_map")
# MAGIC icd_desc=spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.icdcode")
# MAGIC chgmstr=spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.chgmstr")
# MAGIC hospchg = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.hospchg")
# MAGIC cptcode = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.cptcode")
# MAGIC
# MAGIC

# COMMAND ----------

defaultTEXTLIST="PAXLOVID,MOLNUPIR,EVUSHELD,TIXAGEVIMAB,CILGAVIMAB,BEBTELOVIMA,SOTROVIMAB,BAMLANIVIMAB,ETESEVIMAB,REGEN-COV,CASIRIVIMAB,IMDEVIMAB,DEXAMETHASONE,TOFACITINIB,TOCILIZUMAB,SARILUMAB,BARICITINIB,REMDESIVIR,CASIRIVIMAB,IMDEVIMAB,ETESEVIMAB,BAMLANIVIMAB,SOTROVIMAB,BEBTELOVIMAB,PAXLOVID,MOLNUPIRAVIR,REMDESIVIR,"

dbutils.widgets.text("CATALOG",defaultValue="HIVE_METASTORE")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text("SRC_CATALOG",defaultValue="HIVE_METASTORE")
SRC_CATALOG=dbutils.widgets.get("SRC_CATALOG")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="edav_prd_cdh.cdh_sandbox")
SRC_SCHEMA=dbutils.widgets.get("SRC_SCHEMA")

dbutils.widgets.text('SCHEMA',defaultValue='edav_prd_cdh.cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')

dbutils.widgets.text('TEXTLIST',defaultValue=defaultTEXTLIST)
TEXTLIST=dbutils.widgets.get('TEXTLIST').split()

dbutils.widgets.text('FEATURE',defaultValue='COVID_DRUGS')
FEATURE=dbutils.widgets.get('FEATURE')


# COMMAND ----------

import pyspark.sql.functions as F
text_regex='|'.join(list(set((TEXTLIST))))
print(text_regex)

# COMMAND ----------

##fcols=["concept_code","vocabulary_id"]

# COMMAND ----------

tblName=f"{SRC_CATALOG}.{SRC_SCHEMA}.cptcode"
tblCol="CPT_CODE"
tblDesc="CPT_DESC"
tblCode='CPT'
regex=text_regex
cptdrugs=listFilter(tblName,tblCol,tblCode,tblDesc,regex).withColumn("vocabulary_id",F.lit("CPT"))
#.drop("source").toDF(*fcols)
cptdrugs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC cptdrugs=cptcode.filter(F.upper(F.col("CPT_DESC")).rlike(text_regex)).withColumn("vocabulary_id",F.lit("CPT"))#.drop("source").toDF(*fcols)
# MAGIC cptdrugs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC icddrugs=icd_desc.filter(F.upper(F.col("ICD_DESC")).rlike(text_regex)).select("icd_code", "icd_desc").withColumn("vocabulary_id",F.lit("ICD")).drop("source").toDF(*fcols)
# MAGIC icddrugs.display()

# COMMAND ----------

tblName=f"{SRC_CATALOG}.{SRC_SCHEMA}.icdcode"
tblCol="ICD_CODE"
tblDesc="ICD_DESC"
tblCode='ICD'
regex=text_regex
icddrugs=listFilter(tblName,tblCol,tblCode,tblDesc,regex).withColumn("vocabulary_id",F.lit(tblCode))
 #.drop("source").toDF(*fcols)
icddrugs.display()

# COMMAND ----------

tblName=f"{SRC_CATALOG}.{SRC_SCHEMA}.chgmstr"
tblCol="STD_CHG_CODE"
tblDesc="STD_CHG_DESC"
tblCode='CHG'
regex=text_regex
chgdrugs=listFilter(tblName,tblCol,tblCode,tblDesc,regex).withColumn("vocabulary_id",F.lit(tblCode))
#chgdrugs=chgmstr.filter(F.upper(F.col("std_chg_desc")).rlike(text_regex)).select("STD_CHG_CODE").withColumn("vocabulary_id",F.lit("PREMIER_STDCHG")).toDF(*fcols) #.withColumn("feature",F.lit(feature)) 
chgdrugs.display()

# COMMAND ----------

#hospdrugs=hospchg.filter(F.upper(F.col("hosp_chg_desc")).rlike(text_regex)).select("HOSP_CHG_ID").withColumn("vocabulary_id",F.lit("PREMIER_HOSPCHG")).toDF(*fcols)
tblName=f"{SRC_CATALOG}.{SRC_SCHEMA}.hospchg"
tblCol="HOSP_CHG_ID"
tblDesc="HOSP_CHG_DESC"
tblCode='HOSP'
regex=text_regex
hospdrugs=listFilter(tblName,tblCol,tblCode,tblDesc,regex).withColumn("vocabulary_id",F.lit(tblCode))
hospdrugs.display()

# COMMAND ----------

drugfeatures=hospdrugs.unionByName(chgdrugs).unionByName(cptdrugs).unionByName(icddrugs).dropDuplicates().withColumn("feature_name",F.lit(FEATURE))
drugfeatures.display()

# COMMAND ----------

ft_name=f"{CATALOG}.{SCHEMA}.collections"
drugfeatures.write.mode("overwrite").format("delta").saveAsTable(ft_name)
# First one
#ft=spark.table(ft_name)
#fp=ft.union(drugfeatures).dropDuplicates()
#fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)

# COMMAND ----------

# MAGIC %md
# MAGIC schema="edav_prd_cdh.cdh_premier_exploratory"
# MAGIC tbl="ft_drug_features"
# MAGIC otbl=f"{schema}.{tbl}"
# MAGIC ##drugfeatures.write.mode("overwrite").format("delta").saveAsTable(otbl)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC cd /dbfs
# MAGIC wget https://ai2-s2-scispacy.s3-us-west-2.amazonaws.com/data/kbs/2023-04-23/umls_rxnorm_2022.jsonl

# COMMAND ----------

# MAGIC %md
# MAGIC cd /dbfs
# MAGIC cat *.jsonl
