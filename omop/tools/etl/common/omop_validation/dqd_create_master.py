# Databricks notebook source
#make this be a parameter
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#dbutils.widgets.text("atlas_schema",defaultValue="cdh_premier_atlas_results")
dbutils.widgets.text("results_schema",defaultValue="cdh_premier_atlas_results")
#dbutils.widgets.text("omop_edavcdh_catalog",defaultValue="edav_dev_cdh")

dbutils.widgets.text("omop_catalog",defaultValue="edav_stg_cdh")
#admtypeversion = "edav_dev_cdh.cdh_premier_v2.admtype"
#omop_edavcdh_catalog = dbutils.widgets.get("omop_edavcdh_catalog")
results_schema = dbutils.widgets.get("results_schema")
source_schema = dbutils.widgets.get("source_schema")

omop_catalog = dbutils.widgets.get("omop_catalog")
#dqdashboard = spark.sql(f"DESCRIBE HISTORY {admtypeversion} LIMIT 1")
#dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {omop_edavcdh_catalog}.{source_schema}.admtype LIMIT 1")
dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {omop_catalog}.{source_schema}.admtype LIMIT 1")
tblversiondf = dqdashboarddetails.select("version")
latestpremversion = tblversiondf.collect()[0]["version"]
#print(latestpremversion)

# COMMAND ----------

#make this be a parameter
dqdashboardsource = f"{omop_catalog}.{results_schema}.dqdashboard_results"
#dqdashboardsource = "edav_dev_cdh.cdh_premier_atlas_results.dqdashboard_results"
#dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {dqdashboardsource} LIMIT 1")
#dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {dqdashboardsource} LIMIT 1")
#dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {omop_edavcdh_catalog}.{atlas_schema}.dqdashboard_results LIMIT 1")
dqdashboarddetails = spark.sql(f"DESCRIBE HISTORY {omop_catalog}.{results_schema}.dqdashboard_results LIMIT 1")

tblversiondf = dqdashboarddetails.select("version")
latestversion = tblversiondf.collect()[0]["version"]

filterlatestversion = f"""SELECT 
num_violated_rows,
pct_violated_rows,
num_denominator_rows,
execution_time,
query_text,
check_name,
check_level,
check_description,
cdm_table_name,
cdm_field_name,
concept_id,
unit_concept_id,
sql_file,
category,
subcategory,
context,
warning,
error,
checkid,
is_error,
not_applicable,
failed,
passed,
not_applicable_reason,
threshold_value,
notes_value
FROM {dqdashboardsource} VERSION AS OF {latestversion}"""
dqdashbdcurrincr = spark.sql(filterlatestversion)

#dqdashbdcurrincr.show()
#print(latestversion)

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit

dqdint = dqdashbdcurrincr.withColumn("version", lit(latestpremversion))
dqdfinal = dqdint.withColumn("update_timestamp",current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable

#catschemaname = f"{omop_edavcdh_catalog}.{atlas_schema}.dqd_master"
catschemaname = f"{omop_catalog}.{results_schema}.dqd_master"
dqdfinal.dropDuplicates()

#deltaTable = DeltaTable.forName(spark, "edav_dev_cdh.cdh_premier_atlas_results.dqd_master")
deltaTable = DeltaTable.forName(spark, catschemaname)

dqdtempupdate = dqdfinal.createOrReplaceTempView("dqdashbd_updates")
#fr = spark.sql("SELECT * FROM dqdashbd_updates LIMIT 10").collect()
#print(fr)

# COMMAND ----------

spark.sql(f"""INSERT INTO {omop_catalog}.{results_schema}.dqd_master
            (num_violated_rows,
            pct_violated_rows,
            num_denominator_rows,
            execution_time,
            query_text,
            check_name,
            check_level,
            check_description,
            cdm_table_name,
            cdm_field_name,
            concept_id,
            unit_concept_id,
            sql_file,
            category,
            subcategory,
            context,
            warning,
            error,
            checkid,
            is_error,
            not_applicable,
            failed,
            passed,
            not_applicable_reason,
            threshold_value,
            notes_value,
            version,
            update_date)
            SELECT 
            num_violated_rows,
            pct_violated_rows,
            num_denominator_rows,
            execution_time,
            query_text,
            check_name,
            check_level,
            check_description,
            cdm_table_name,
            cdm_field_name,
            concept_id,
            unit_concept_id,
            sql_file,
            category,
            subcategory,
            context,
            warning,
            error,
            checkid,
            is_error,
            not_applicable,
            failed,
            passed,
            not_applicable_reason,
            threshold_value,
            notes_value,
            version,
            update_timestamp
            FROM dqdashbd_updates
          """)
