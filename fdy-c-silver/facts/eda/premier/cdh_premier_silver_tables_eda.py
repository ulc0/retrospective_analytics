# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS  edav_prd_cdh.cdh_premier_exploratory
# MAGIC      COMMENT 'Premier Featurization Silver Schema' 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE edav_prd_cdh.cdh_premier_exploratory

# COMMAND ----------

# DBTITLE 1,IntegerOverflow --- should we address this?
# MAGIC %sql
# MAGIC select
# MAGIC -- if(medrec_key<0,medrec_key+2147483647,medrec_key) as pmedrec_key,MEDREC_KEY
# MAGIC min(medrec_key),max(medrec_key),min(PAT_KEY),max(PAT_KEY),count(*)
# MAGIC from edav_prd_cdh.cdh_premier_v2.patdemo
# MAGIC --order by MEDREC_KEY

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), count(distinct pat_key) from edav_prd_cdh.cdh_premier_v2.paticd_diag

# COMMAND ----------

# MAGIC %sql
# MAGIC select FACILITY_TEST_NAME,lab_test,lab_test_result,count(*) as records
# MAGIC from edav_prd_cdh.cdh_premier_v2.vitals
# MAGIC where LAB_TEST_LOINC_CODE is NULL
# MAGIC group by FACILITY_TEST_NAME,LAB_TEST,ABNORMAL_FLAG,LAB_TEST_RESULT
# MAGIC order by facility_test_name,records,LAB_TEST_RESULT desc

# COMMAND ----------


        # Specifying some columns to pull
genlab_cols = [
            'pat_key', 'collection_day_number', 'collection_time_of_day',
            'lab_test_loinc_desc', 'lab_test_result', 'numeric_value'
        ]
vital_cols = [
            'pat_key', 'episode_day_number', 'episode_time_of_day',
            'lab_test', 'test_result_numeric_value'
        ]
bill_cols = ['pat_key', 'std_chg_desc', 'serv_day']
lab_res_cols = [
            'pat_key', 'spec_day_number', 'spec_time_of_day', 'test',
            'observation'
        ]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC demo_target = (target_enc
# MAGIC     .select("pat_key")
# MAGIC     .join(demo.select("pat_key","age","race","gender","hispanic_ind"), on = "pat_key", how="left")
# MAGIC    )
# MAGIC
# MAGIC
# MAGIC
# MAGIC # bin age
# MAGIC # - bins age by 10 years
# MAGIC def get_age_bin(sdf):
# MAGIC age_buc = (F.when((F.col("age") >=0) & (F.col("age") < 10), "0-9")
# MAGIC     .when((F.col("age") >=10) & (F.col("age") < 20), "10-19")
# MAGIC     .when((F.col("age") >= 20) & (F.col("age") < 30), "20-29")
# MAGIC     .when((F.col("age") >= 30) & (F.col("age") < 40), "30-39")
# MAGIC     .when((F.col("age") >= 40) & (F.col("age") < 50), "40-49")
# MAGIC     .when((F.col("age") >= 50) & (F.col("age") < 60), "50-59")
# MAGIC     .when((F.col("age") >= 60) & (F.col("age") < 70), "60-69")
# MAGIC     .when((F.col("age") >= 70) & (F.col("age") < 80), "70-79")
# MAGIC     .when((F.col("age") >= 80) & (F.col("age") < 90), "80-89")
# MAGIC     .when((F.col("age") >= 90) & (F.col("age") < 100), "90-99")
# MAGIC     .otherwise(">=100")
# MAGIC    )
# MAGIC out = (sdf
# MAGIC .withColumn("age_bin", age_buc)
# MAGIC .drop("age")
# MAGIC )
# MAGIC return(out)
