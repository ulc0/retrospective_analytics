-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

-- COMMAND ----------

use edav_prd_cdh.cdh_premier_exploratory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE WIDGET TEXT "BRONZE" DEFAULT "edav_prd_cdh.cdh_premier"
-- MAGIC CREATE WIDGET TEXT "SILVER" DEFAULT "edav_prd_cdh.cdh_premier_exploratory"

-- COMMAND ----------

select DISTINCT sum_dept_desc,std_dept_desc, std_dept_code
from edav_prd_cdh.cdh_premier_v2.chgmstr
order by sum_dept_desc,Std_DEPT_DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC sum_dept_desc
-- MAGIC CARDIOLOGY/EKG
-- MAGIC ER
-- MAGIC LAB
-- MAGIC LABOR AND DELIVERY
-- MAGIC OR
-- MAGIC OTHER
-- MAGIC PHARMACY
-- MAGIC RADIOLOGY
-- MAGIC RESPIRATORY
-- MAGIC ROOM AND BOARD
-- MAGIC SUPPLY
-- MAGIC THERAPY
-- MAGIC UNKNOWN

-- COMMAND ----------

select * from edav_prd_cdh.cdh_premier_v2.chgmstr
where sum_dept_desc = "LAB"

-- COMMAND ----------

select * from edav_prd_cdh.cdh_premier_v2.chgmstr
where sum_dept_desc = "CARDIOLOGY/EKG"

-- COMMAND ----------

select DISTINCT sum_dept_desc,std_dept_code, std_dept_desc
from edav_prd_cdh.cdh_premier_v2.chgmstr
order by SUM_DEPT_DESC


-- COMMAND ----------

create or replace table edav_prd_cdh.cdh_premier_exploratory.fs_patbill_skinny
as select DISTINCT STD_CHG_CODE, HOSP_CHG_ID
from edav_prd_cdh.cdh_premier_v2.patbill
order by STD_CHG_CODE

-- COMMAND ----------

select std_chg_code,count(*) as num_hospchg
from edav_prd_cdh.cdh_premier_exploratory.fs_patbill_skinny
group by std_chg_code
order by num_hospchg desc



-- COMMAND ----------

-- MAGIC %md
-- MAGIC from pyspark.sql.functions import collect_list
-- MAGIC patbill_skinny=spark.table("edav_prd_cdh.cdh_premier_v2.cghmstr")
-- MAGIC  = df.groupBy("name").agg(collect_list("languages") \
-- MAGIC     .alias("languages"))
-- MAGIC df2.printSchema()    
-- MAGIC df2.show(truncate=False)

-- COMMAND ----------



-- COMMAND ----------

create  or replace table edav_prd_cdh.cdh_premier_exploratory.fs_pharmacy as
select distinct c.* --,p.hosp_chg_id --, h.* 
from --patbill_skinny p
edav_prd_cdh.cdh_premier_v2.chgmstr c
join edav_prd_cdh.cdh_premier_v2.hospchg h
where c.std_chg_code in (select DISTINCT STD_CHG_CODe from pharmacy_codes)
---order by c.PROD_CAT_CODE, c.PROD_CLASS_CODE, c.PROD_NAME_CODE, c.PROD_NAME_METH_CODE

-- COMMAND ----------

create or replace table clinical_nonpharm as
select distinct c.*, p.hosp_chg_id --, h.* 
from patbill_skinny p
join edav_prd_cdh.cdh_premier_v2.chgmstr c
--join edav_prd_cdh.cdh_premier_v2.hospchg h
where c.STD_DEPT_CODE<>'250'

