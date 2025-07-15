# Databricks notebook source
import pyspark.sql.functions as F



# COMMAND ----------

dbutils.widgets.text('SRC_CATALOG',
    defaultValue='edav_prd_cdh')
SRC_CATALOG=dbutils.widgets.get('SRC_CATALOG')

dbutils.widgets.text('CATALOG',
    defaultValue='edav_prd_cdh')
CATALOG=dbutils.widgets.get('CATALOG')

dbutils.widgets.text('SRC_SCHEMA',
    defaultValue='cdh_reference_data')
SRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')

dbutils.widgets.text('SCHEMA',
    defaultValue='cdh_sandbox')
SCHEMA=dbutils.widgets.get('SCHEMA')

# COMMAND ----------

#edav_prd_cdh.cdh_reference_data.ccsr_dx_codelist
ccsr_table=f"{SRC_CATALOG}.{SRC_SCHEMA}.ccsr_dx_codelist"
print(ccsr_table)



# COMMAND ----------

newcols=['concept_code','CONCEPT_TEXT','feature_name','FEATURE_DESC','Inpatient_Default','outpatient','rationale','version'
]
ccsr_lookup = spark.table(ccsr_table).toDF(*newcols).select('concept_code','feature_name').withColumn('vocabulary_id',F.lit('ICD'))

# COMMAND ----------

#display(ccsr_lookup)


# COMMAND ----------


display(ccsr_lookup.where(ccsr_lookup.feature_name == 'INF006'))

# COMMAND ----------

ft_name=f"{CATALOG}.{SCHEMA}.collections"
#drugfeatures.write.format("delta").saveAsTable(ft_name)
#ft=spark.table(ft_name)
#fp=ft.union(ccsr_lookup) #.dropDuplicates()
#fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)

# COMMAND ----------

write_mode = "append"
schema_option = "mergeSchema"
 
ccsr_lookup.write.format("delta").mode(write_mode).option(
                schema_option, "true"
            ).saveAsTable( ft_name)

# COMMAND ----------

fp=spark.table(ft_name)
display(fp.where(fp.feature_name == 'INF006'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # PySpark Column to List
# MAGIC ICD_LIST=t.rdd.map(lambda x: x[10]).collect()
# MAGIC
# MAGIC
# MAGIC #['CA', 'NY', 'CA', 'FL']
# MAGIC
# MAGIC
# MAGIC #ccsrs.select("ICD_CODE").collect()
# MAGIC #print(ICD_LIST)
