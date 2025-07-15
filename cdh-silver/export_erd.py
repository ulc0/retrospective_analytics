# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from itertools import chain

# COMMAND ----------

"""
output format

CREATE TABLE [EntityName] (
  [Field1] Type1,
  [Field2] Type2,
  [Field3] Type
);

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   system.information_schema.columns
# MAGIC WHERE
# MAGIC   table_catalog = 'edav_prd_cdh
# MAGIC ```
# MAGIC
# MAGIC | table_catalog | table_schema  | table_name               | column_name | ordinal_position | column_default | is_nullable | full_data_type | data_type | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type | interval_precision | maximum_cardinality | is_identity | identity_generation | identity_start | identity_increment | identity_maximum | identity_minimum | identity_cycle | is_generated | generation_expression | is_system_time_period_start | is_system_time_period_end | system_time_period_timestamp_generation | is_updatable | partition_index | comment |
# MAGIC |---------------|---------------|--------------------------|-------------|------------------|----------------|-------------|----------------|-----------|--------------------------|------------------------|-------------------|-------------------------|---------------|--------------------|---------------|--------------------|---------------------|-------------|---------------------|----------------|--------------------|------------------|------------------|----------------|--------------|-----------------------|-----------------------------|---------------------------|-----------------------------------------|--------------|-----------------|---------|
# MAGIC | edav_prd_cdh  | cdh_abfm_deid | generatedpatientbaseline | patientuid  | 0                | null           | YES         | string         | STRING    | 0                        | 0                      | null              | null                    | null          | null               | null          | null               | null                | NO          | null                | null           | null               | null             | null             | null           | NO           | null                  | NO                          | NO                        | null                                    | YES          | null            | null    |
# MAGIC | edav_prd_cdh  | cdh_abfm_deid | generatedpatientbaseline | gender      | 1                | null           | YES         | string         | STRING    | 0                        | 0                      | null              | null                    | null          | null               | null          | null               | null                | NO          | null                | null           | null               | null             | null             | null           | NO           | null                  | NO                          | NO                        | null                                    | YES          | null            | null    |

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT 'mysql' dbms,  
# MAGIC t.TABLE_SCHEMA,  
# MAGIC t.TABLE_NAME,  
# MAGIC c.COLUMN_NAME,  
# MAGIC c.ORDINAL_POSITION,  
# MAGIC c.DATA_TYPE,  
# MAGIC c.CHARACTER_MAXIMUM_LENGTH,  
# MAGIC n.CONSTRAINT_TYPE,  
# MAGIC k.REFERENCED_TABLE_SCHEMA,  
# MAGIC k.REFERENCED_TABLE_NAME,  
# MAGIC k.REFERENCED_COLUMN_NAME   

# COMMAND ----------

tables_list=['edav_project_audit_logs.cdh.column_lineage',] #TODO
table_catalogs=["edav_prd_cdh",]
schemas=["cdh_truveta",]
info_schema="edav_prd_cdh.information_schema.columns"

# COMMAND ----------

#   .filter(F.col("table_schema").contains(schemas))\
#table_window = Window.orderBy("table_schema","table_name")
#    .withColumn("table_number", F.row_number().over(table_window))
#    .filter(F.col("table_catalog").isin(*table_catalogs))\
system_columns_df=spark.table(info_schema)\
    .filter(F.col("table_schema").isin(schemas))\
    .withColumn("table_full_name",F.concat(F.col("table_schema"),F.lit('.'),F.col("table_name"),))\
    .orderBy(["table_name",'ordinal_position'])
display(system_columns_df)

# COMMAND ----------

#TODO Function
col="column_name"
table_cols_df=system_columns_df.groupBy(col)\
    .agg(F.countDistinct('table_full_name').alias('collected'))\
        .filter(F.col("collected")>1)
#        .select(col)\
#        .toPandas()[col].tolist()
display(table_cols_df)
table_cols=[row["column_name"] for row in table_cols_df.select('column_name').collect()]


# COMMAND ----------

# DBTITLE 1,Mermaid Format
mermaidString="""
        ORDER {
        int orderNumber
        string deliveryAddress
    }
    LINE-ITEM {
        string productCode
        int quantity
        float pricePerUnit
    }
     {
        int orderNumber
        string deliveryAddress
    }
    LINE-ITEM {
        string productCode
        int quantity
        float pricePerUnit
    }
"""


# COMMAND ----------

sparkToMermaidDict={
    "DATE"      :"date   ",
    "STRING"    :"string ",
    "FLOAT"     :"float  ",
    "LONG"      :"long   ",
    "INT"       :"int    ",
    "DOUBLE"    :"double ",
    "BOOLEAN"   :"boolean",
}
sparkToMermaidMap = F.create_map([F.lit(x) for x in chain(*sparkToMermaidDict.items())])

# COMMAND ----------

# DBTITLE 1,Allegedly Suitable for LucidChart
#    .withColumn('is_key',F.col("column_name").isin(table_cols))\
from pyspark.sql.functions import when
w = Window.partitionBy('table_name').orderBy('ordinal_position')
cdh_cols=system_columns_df.select(['table_schema','table_name','ordinal_position','column_name','data_type',])\
    .drop("table_schema")\
    .withColumn('bold',(F.when(F.col("column_name").isin(table_cols), '**').otherwise('')))\
    .withColumn('data_type',sparkToMermaidMap[F.col("data_type")])\
    .withColumn("mermaidCol",F.concat(F.col("data_type"),F.lit(" "),F.col("bold"),F.col("column_name"),F.col("bold"),))\
    .withColumn('column_array',F.collect_list('mermaidCol').over(w))\
    .groupBy('table_name').agg(F.max('column_array').alias('column_array'))
display(cdh_cols)

# COMMAND ----------

erdDict=(cdh_cols.toPandas().to_dict("records"))


# COMMAND ----------

print(erdDict[0])

# COMMAND ----------

mermaidFile=f"./mermaids/{schemas}.md"
# each line is a list item
mermaidList=["```mermaid","erDiagram"]
tableIndent=" "*3
colIndent=" "*5

for entry in erdDict:
    table_name=entry["table_name"].lower()
    print(table_name)
    mermaidList=mermaidList+[tableIndent+table_name+" {"]
    for col in entry["column_array"]:
        mermaidList=mermaidList+[colIndent+col.lower()]
    mermaidList=mermaidList+["}"]
mermaidList=mermaidList+["```"]


# COMMAND ----------


with open(mermaidFile,'w') as file:
   for m in mermaidList:
        file.write(str(m) +"\n")

# COMMAND ----------

# DBTITLE 1,supposed lucidchart format
# MAGIC %md
# MAGIC %sql
# MAGIC
# MAGIC SELECT 'spark' dbms,
# MAGIC TABLE_SCHEMA,
# MAGIC TABLE_NAME,
# MAGIC COLUMN_NAME,
# MAGIC ORDINAL_POSITION,
# MAGIC DATA_TYPE,
# MAGIC CHARACTER_MAXIMUM_LENGTH,
# MAGIC " " as CONSTRAINT_TYPE,
# MAGIC " " as REFERENCED_TABLE_SCHEMA,
# MAGIC " " as REFERENCED_TABLE_NAME,
# MAGIC " " as REFERENCED_COLUMN_NAME
# MAGIC FROM
# MAGIC   system.information_schema.columns
# MAGIC WHERE
# MAGIC   table_catalog = 'edav_prd_cdh'
# MAGIC   and table_schema='cdh_engineering_etl'
