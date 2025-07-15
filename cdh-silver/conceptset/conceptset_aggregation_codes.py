# Databricks notebook source
dbutils.widgets.text("CATALOG",defaultValue="edav_prd_cdh")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text("SRC_CATALOG",defaultValue="edav_prd_cdh")
SRC_CATALOG=dbutils.widgets.get("SRC_CATALOG")
dbutils.widgets.text('SRC_SCHEMA',defaultValue='edav_prd_cdh.cdh_reference_data')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text('SCHEMA',defaultValue='edav_prd_cdh.cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')
FEATURE='feature_name'

# COMMAND ----------

## lifted wholesale from https://github.com/StefanoTrv/simple_icd_10/blob/master/package-files/simple_icd_10/simple_icd_10.py
def _add_dot_to_code(icdcode):
    if len(icdcode)<4 or icdcode[3]==".":
        return icdcode
    elif icdcode[:3]+"."+icdcode[3:] in code_to_node:
        return icdcode[:3]+"."+icdcode[3:]
    else:
        return icdcode



# COMMAND ----------

# This should be cleaned up and standardized once edav_prd_cdh.cdh_sandbox has a standarizaed CCSR list maintained
# Pattern follows for other aggregators. GROUPING_CONCEPT_FEATURE
featureCCSRDict={"CCSR_ICD_CCSRCOVID1" : ["END005",
        "CIR013",
        "RSP010",
        "NVS014",
        "NEO059",
        "NEO064",
        "CIR005",
        "NVS018",
        "CIR015",
        "RSP002",
        "INF002",
        ],
"CCSR_ICD_CCSRCOVID2" : ["END005",
        "RSP010",
        "NVS014",
        "CIR005",
        "CIR015",
        "RSP002",],


}


# COMMAND ----------

#this can be a dictionary of dictionaries, or sometihng
#"CCSR_ICD_CCSRCOVID2" 

ltables={
    "CCSR":"ccsr_dx_codelist",
}
lcolumns={
    "ICD":"icd_10_cm_code",
}
fcolumn={
    "CCSR":"ccsr_Category",
}


# COMMAND ----------

#ccsrs=spark.table(f"{CATALOG.{SRC_SCHEMA}.ccsr_icd_codes")
for feature_code,ccsrList in featureCCSRDict.items():
    tbl,ct,fn=feature_code.split("_")
    ftable=ltables[tbl]
    concept_column=lcolumns[ct]
    vlist="|".join(ccsrList)
    catcol=fcolumn[tbl]
    #sqlstring=f"select distinct {concept_column} as code,'{fn}' as {FEATURE}, '{ct}' as vocabulary_id from {SRC_CATALOG}.{SRC_SCHEMA}.{ftable} where {catcol} RLIKE '{vlist}'"
    sqlstring=f"select distinct {concept_column} as code,'{fn}' as {FEATURE}, '{ct}' as vocabulary_id from {SRC_CATALOG}.{SRC_SCHEMA}.{ftable} where {catcol} RLIKE '{vlist}'"
    print(sqlstring) 
    values=spark.sql(sqlstring)
    print(values)
    ft_name=f"{CATALOG}.{SCHEMA}.concept_sets"
    write_mode = "append"
    schema_option = "mergeSchema"
    values.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable( ft_name)
    #fp=spark.table(ft_name)
    #fp=fp.unionByName(values).dropDuplicates()
    #fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)
#display(fp)

