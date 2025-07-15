# Databricks notebook source
oschema="edav_prd_cdh.cdh_premier_exploratory"
schema="edav_prd_cdh.cdh_premier_v2"

# COMMAND ----------

xref_tbl=f"{oschema}.ml_hospchg_stdcode_xref"
xref=spark.sql(f"select distinct p.hosp_chg_id, p.std_chg_code from {schema}.patbill p") 
#join edav_prd_cdh.cdh_premier_v2.hospchg h order by p.std_chg_code")
xref.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(xref_tbl)

# COMMAND ----------

xref=spark.table(xref_tbl)
xref.display()

# COMMAND ----------

sqlstring=f"select distinct s.*,hosp_chg_desc from {xref_tbl} s join {schema}.chgmstr  join {schema}.hospchg"
fullxref=spark.sql(sqlstring)
fullxref.display()

# COMMAND ----------

sqlstring=f"select * from edav_prd_cdh.cdh_premier_v2.hospchg where hosp_chg_id in (select distinct hosp_chg_id from {xref_tbl} where std_chg_code='99999') "
oddid=spark.sql(sqlstring)
oddid.display()
