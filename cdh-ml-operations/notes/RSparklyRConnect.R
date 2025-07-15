# https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/r/

library(sparklyr)
library(dplyr)
library(dbplyr)

sc <- sparklyr::spark_connect(
  master     = "https://adb-8219004871211837.17.azuredatabricks.net/",
  cluster_id = "0427-171205-6qa2hhxn",
  token      = "<PAT>",
  method     = "databricks_connect",
  envname    = "r-sparklyr-databricks-14.2"
)

trips <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("samples", "nyctaxi", "trips")
)

print(trips, n = 5)
