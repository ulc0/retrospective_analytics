# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from itertools import chain

# COMMAND ----------

lineage_tbl='edav_project_audit_logs.cdh.lineage'
schemas_tbl='edav_prd_cdh.information_schema.tables'
table_catalogs=["edav_prd_cdh",]
schema="cdh_truveta_lava_dev"
mermaid_name=f"../mermaids/{schema}"

# COMMAND ----------

tables_df=spark.table(schemas_tbl).filter(F.col("table_schema")==schema)\
    .withColumn("table_full_name",F.concat(F.col("table_schema"),F.lit('.'),F.col("table_name"),))\
    .select("table_full_name").dropDuplicates()\
    .withColumn("has_lineage",F.lit(1))
display(tables_df)

# COMMAND ----------

#   .filter(F.col("table_schema").contains(schemas))\
#table_window = Window.orderBy("table_schema","table_name")
#    .withColumn("table_number", F.row_number().over(table_window))
#    .filter(F.col("table_catalog").isin(*table_catalogs))\

# only tables with use
# need to collect the entity_types
lineage_df=spark.table(lineage_tbl).select('source_table_schema','target_table_schema','entity_type','entity_id','source_table_name','target_table_name')\
    .filter((F.col("source_table_schema")==F.lit(schema))|(F.col("target_table_schema")==F.lit(schema)))\
    .withColumn("source_table_full_name",F.concat(F.col("source_table_schema"),F.lit('.'),F.col("source_table_name"),))\
    .withColumn("target_table_full_name",F.concat(F.col("target_table_schema"),F.lit('.'),F.col("target_table_name"),))\
    .join(tables_df.withColumnRenamed("table_full_name","source_table_full_name")\
                 .withColumnRenamed("has_lineage","has_source_lineage"),"source_table_full_name","left")\
    .join(tables_df.withColumnRenamed("table_full_name","target_table_full_name")\
                    .withColumnRenamed("has_lineage","has_target_lineage"),"target_table_full_name","left")\
    .filter(F.col("source_table_schema").isNotNull())\
    .filter(F.col("source_table_full_name")!=F.col("target_table_full_name"))\
    .withColumn("is_source_schema",F.when(F.col("source_table_schema")==schema,F.lit(1)))\
    .withColumn("is_target_schema",F.when(F.col("target_table_schema")==schema,F.lit(1)))\
    .dropDuplicates()\
    .orderBy(["source_table_full_name","entity_id"])
display(lineage_df)

# COMMAND ----------

NOTEBOOK_PREFIX="https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/"
DBLSQL_PREFIX="https://adb-5189663741904016.16.azuredatabricks.net/sql/editor/"

# COMMAND ----------

from graphframes import *
#display(cols_tables_df)
tblVertices=lineage_df.withColumn("link",
    F.when(F.col("entity_type")=="NOTEBOOK",F.concat(F.lit(NOTEBOOK_PREFIX),F.col("entity_id")))\
    .when(F.col("entity_type")=="DBLSQL_QUERY",F.concat(F.lit(DBLSQL_PREFIX),F.col("entity_id")))
    .otherwise(""))\
#    .select('source_table_full_name','target_table_full_name','link')\


#  .withColumnRenamed('table_name','dst_table_name')\
#  .filter(~F.contains(F.col('dst_table_name'),F.col('src_table_name')))


display(tblVertices)
linksDict=(tblVertices.toPandas().to_dict("records"))
print(linksDict)
#tblVertices = airports.withColumnRenamed("IATA", "id").distinct()
#tblEdges = col_table_df.select("tblid", "delay", "src", "dst", "city_dst", "state_dst")

# This GraphFrame builds upon the vertices and edges based on our trips (flights)
#tblGraph = GraphFrame(tblVertices, tblEdges)

# COMMAND ----------

tbl_schemas=lineage_df.select("source_table_schema").dropDuplicates().union(lineage_df.select("target_table_schema").dropDuplicates()).dropDuplicates()
schemasDict=(tbl_schemas.toPandas().to_dict("records"))
print(schemasDict)


# COMMAND ----------

schemaList=[row['source_table_schema'] for row in tbl_schemas.select('source_table_schema').collect()]
print(schemaList)

# COMMAND ----------

mermaidFcFile=f"./mermaids/{mermaid_name}_schemas_chart.md"
# each line is a list item
#mermaidErdList=["```mermaid","erDiagram"]
mermaidFcList=["```mermaid","graph TD"]
tableIndent=" "*3
colIndent=" "*5

for schema in schemaList:
#    mermaidFcList=mermaidFcList+[f"subgraph {schema}"]
    tblList=[]
    #    mermaidErdList=mermaidErdList+[tableIndent+table_name+" {"]
    mermaidFcList=mermaidFcList+["subgraph "+schema]
    for entry in linksDict:
        src_schema=entry['source_table_schema']
        tar_schema=entry['target_table_schema']
        if (src_schema==schema):
            tblList=tblList+[entry['source_table_name']]
        if (tar_schema==schema):
            tblList=tblList+[entry['target_table_name']]
    for tbl in list(set(tblList)):
        mermaidEntry=[colIndent+tbl.lower()]
        mermaidFcList=mermaidFcList+mermaidEntry
    mermaidFcList=mermaidFcList+["END"]
   
for link in linksDict:
    print(link)
    entity_type=link["entity_type"]
    if entity_type is not None:
        src_table=link["source_table_name"].lower()
        dst_table=link["target_table_name"].lower()
        link_name=entity_type+' '+link["entity_id"]
        mermaidFcList=mermaidFcList+[f"{src_table}--{link_name}--{dst_table}"]
#    mermaidErdList=mermaidErdList+[f"{src_table}-->{link_name}>--{dst_table}"]
  
#mermaidErdList=mermaidErdList+["```"]
 
#mermaidFcList=mermaidFcList+["```"]


# COMMAND ----------


with open(mermaidFcFile,'w') as file:
   for m in mermaidFcList:
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
