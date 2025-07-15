# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ##### 08/21/2024 - nishant: reading and writing from/to a UC database ONLY works when running on an assigned cluster and will nit work on an isolated access mode (UC not supported) or shared (R is not supported)
# MAGIC
# MAGIC - In this test case the attached cluster used was CDH_Cluster_Python_SQL_UC with assignment to me
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Setup

# COMMAND ----------

#Enable Databricks display() for sparklyr tibbles
display_sparklyr_view <- function(view) {
  sparklyr::sdf_register(view, 'tmp')
  d <- SparkR::tableToDF('tmp') %>% display()
  SparkR::sql("DROP VIEW IF EXISTS tmp")
  return(d)
}

# COMMAND ----------

library(sparklyr)
library(dplyr)

# COMMAND ----------

# MAGIC %md
# MAGIC #### sparklyr
# MAGIC Attempting to save table to exploratory db

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Scenario 1 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attempting to read a table works

# COMMAND ----------

sc <- spark_connect(method = 'databricks')
# connect to UC database
tbl_change_db(sc, 'edav_dev_cdh.cdh_ml')

test_tbl <- spark_read_table(sc, 'premier_drugs') %>% sdf_sample(fraction=.00001)
display_sparklyr_view(test_tbl)

# COMMAND ----------

sdf_register(test_tbl, "tmp_test_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write a UC table also works
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

spark_write_table(test_tbl, name = 'edav_dev_cdh.cdh_sandbox.nishant',
                  mode = 'overwrite',
                  options = list('overwriteSchema' = 'true'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### load the custom package (testing)
# MAGIC

# COMMAND ----------

# library(sparklyr)
install.packages("surveytable")
