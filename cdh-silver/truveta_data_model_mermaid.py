# Databricks notebook source
#read in the .csv from Truvuta
import pandas as pd
import pyspark.sql.functions as F
tdm_csv="./TDM_abbreviated.csv"
tdm_df=spark.createDataFrame(pd.read_csv(tdm_csv))
display(tdm_df)
mermaid_name="truveta_data_model"


# COMMAND ----------

lineage_tbl='edav_project_audit_logs.cdh.lineage'
table_catalogs=["edav_prd_cdh",]
schemas=["cdh_truveta",]#"cdh_truveta_lava_dev"]
info_schema="edav_prd_cdh.information_schema.columns"
mermaid_name='truveta'

# COMMAND ----------

keys_df=tdm_df.filter((F.col("PrimaryKey")==F.lit(1)))
fkey_df=tdm_df.filter(F.isnotnull(F.col("ForeignKey")))
display(keys_df)
display(fkey_df)

# COMMAND ----------

keysDictList=(keys_df.toPandas().to_dict("records"))
fkeyDictList=(fkey_df.toPandas().to_dict("records"))



# COMMAND ----------

mermaidErdFile = f"{mermaid_name}_erd.md"
mermaidFcFile = f"{mermaid_name}_chart.md"
# each line is a list item
mermaidErdList = ["```mermaid", "erDiagram"]
mermaidFcList = ["```mermaid", "graph LR"]
tableIndent = " " * 3
subIndent = " " * 5
colIndent = " " * 7

for entry in keysDictList:
    table_name = entry["Table"].lower()
    pkey = table_name + "." + entry["Name"].lower()
    # print(table_name)
#    mermaidFcList = mermaidFcList + [subIndent + "subgraph TD " + table_name]
    mermaidFcList = mermaidFcList + [subIndent + pkey]
#    mermaidFcList = mermaidFcList + [subIndent + "end"]

for link in fkeyDictList:
    src_table = link["Table"].lower()
    dst_table = link["ForeignKey"].lower()
    link_name = link["Name"].lower()
    dests = dst_table.split(",")
    for d in dests:
        #    mermaidErdList=mermaidErdList+[f"{src_table}--{link_name}--{dst_table}"]
        print(d)
        mermaidFcList = mermaidFcList + [f"{src_table}--{link_name}--{d} "]
#      mermaidFcList=mermaidFcList+[subIndent+f"{src_table}--> {d} "]
mermaidFcList = mermaidFcList + ["```"]

# COMMAND ----------


print(mermaidFcFile)
with open(mermaidFcFile,'w') as file:
   for m in mermaidFcList:
        file.write(str(m) +"\n")
with open(mermaidErdFile,'w') as file:
   for m in mermaidErdList:
        file.write(str(m) +"\n")

