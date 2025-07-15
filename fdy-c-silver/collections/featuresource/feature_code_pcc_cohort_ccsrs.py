# Databricks notebook source
def listFilter(tblName,tblCol,regex):
    codesql=f"select *,'{tblName}' as source from {tblName} where {tblCol} RLIKE '{regex}' "
    testfile=spark.sql(codesql)
    return testfile

# COMMAND ----------

ccsr_table="edav_prd_cdh.cdh_ml_test.icd10cm_diagnosis_codes_to_ccsr_categories_map"
ccsr_lookup = spark.table(ccsr_table)
icd_desc=spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.icdcode")
CCSR_COVID1 = ["END005",
        "CIR013",
        "RSP010",
        "NVS014",
        "NEO059",
        "NEO064",
        "CIR005",
        "NVS018",
        "CIR015",
        "RSP002",
        "INF002", ]
CCSR_COVID2 = ["END005",
        "RSP010",
        "NVS014",
        "CIR005",
        "CIR015",
        "RSP002",]


# COMMAND ----------

import pyspark.sql.functions as F
regex='|'.join(list(set(CCSR_COVID1+CCSR_COVID2)))
print(regex)

# COMMAND ----------



t=listFilter(ccsr_table,"CCSR_Category",regex).withColumn("ICD_CODE1",F.substring('ICD-10-CM_Code',1,3)).withColumn("ICD_CODE2",F.substring(ljust('ICD-10-CM_Code',5), 4,5)).withColumn("ICD_CODE",F.concat_ws('.',F.col("ICD_CODE1"),F.col("ICD_CODE2"))).withColumn("CODE",F.lit("ICD"))
t.display()

# COMMAND ----------

ccsrs=t.select("CODE","ICD_CODE","CCSR_Category","CCSR_Category_Description")
ccsrs.display()

# COMMAND ----------


# PySpark Column to List
ICD_LIST=t.rdd.map(lambda x: x[10]).collect()


#['CA', 'NY', 'CA', 'FL']


#ccsrs.select("ICD_CODE").collect()
#print(ICD_LIST)

# COMMAND ----------

matching=icd_desc.filter(icd_desc.ICD_CODE.isin(ICD_LIST))
matching.display()

# COMMAND ----------

oj=ccsrs.join(icd_desc,ccsrs.ICD_CODE ==  icd_desc.ICD_CODE,"leftouter")
oj.display()
