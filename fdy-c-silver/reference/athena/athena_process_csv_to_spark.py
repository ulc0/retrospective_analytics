# Databricks notebook source
dbutils.widgets.text("source","/Volumes/edav_prd_cdh/cdh_ml/metadata_data/athena/v20240830/")
dbutils.widgets.text("dest","edav_prd_cdh.cdh_ml")


# COMMAND ----------

SOURCE=dbutils.widgets.get("source")
DEST=dbutils.widgets.get("dest")
print(SOURCE)
print(DEST)

# COMMAND ----------

flist=[f.path for f in dbutils.fs.ls(SOURCE)]
print(flist)

# COMMAND ----------

concept_df=spark.table(f"{DEST}.concept_old")
display(concept_df)
print(concept_df.schema.json())

# COMMAND ----------

import json

for csv in flist:
    fname=csv.split('/')[-1]
    tname=fname.split('.')[0]
    dfname=f"{DEST}.{tname}"
    df=spark.read.option("delimiter",'\t').option("header","true").csv(csv)
    #print(df.schema)
    jschema=json.loads(df.schema.json())
    #print(jschema)
    schemapath = f"{tname}.json"
    with open(schemapath,'w') as f:
        d = json.dump(jschema, f, indent=4, sort_keys=False)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
tbls=['concept','concept_relationship']
dvol="dbfs:/Volumes/edav_prd_cdh/cdh_ml/metadata_data/athena/v20240830"
for tname in tbls:
#    fname=csv.split('/')[-1]
#    tname=fname.split('.')[0]
    fname=f"{tname.upper()}.csv"
    csv=f"{dvol}/{fname}"
    print(fname)
    dfname=f"{DEST}.{tname}"
#    df=spark.read.option("delimiter",'\t').option("header","true").csv(csv)
    df=spark.read.option("delimiter",'\t').option("header","true").csv(csv)
    print(df.schema)
#    if (tbls=='concept'):
#        df=df.withColumn('concept_id_bigint',col('concept_id').cast(LongType())).drop("concept_id").withColumnRenamed("concept_id_bigint","concept_id")
#    else:
#        df=df.withColumn('concept_id_bigint',col('concept_id_1').cast(LongType())).drop("concept_id_1").withColumnRenamed("concept_id_bigint","concept_id_1")
#        df=df.withColumn('concept_id_bigint',col('concept_id_2').cast(LongType())).drop("concept_id_2").withColumnRenamed("concept_id_bigint","concept_id_2")
    display(df)
    write_mode = "overWrite"
    schema_option = "mergeSchema"
    df.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable(f"{DEST}.{tname}_raw")
    jschema=json.loads(df.schema.json())
    #print(jschema)
    schemapath = f"{tname}.json"
    with open(schemapath,'w') as f:
        d = json.dump(jschema, f, indent=4, sort_keys=False)
    



# COMMAND ----------

tname='concept'
df_concept=spark.sql(f"select *, cast(concept_id as LONG) as concept_id_long from {DEST}.{tname}_raw").drop("concept_id").withColumnRenamed("concept_id_long","concept_id")
display(df_concept)
df_concept.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable(f"{DEST}.{tname}")


# COMMAND ----------

tname='concept_relationship'
df_concept_rel=spark.sql(f"select *, cast(concept_id_1 as LONG) as concept_id_1_long, cast(concept_id_2 as LONG) as concept_id_2_long from {DEST}.{tname}_raw").drop("concept_id_1").withColumnRenamed("concept_id_1_long","concept_id_1").drop("concept_id_2").withColumnRenamed("concept_id_2_long","concept_id_2")
display(df_concept_rel)
df_concept_rel.write.format("delta").mode(write_mode).option(schema_option, "true").saveAsTable(f"{DEST}.{tname}")


# COMMAND ----------

# MAGIC %md
# MAGIC schemapath = '/path/spark-schema.json'
# MAGIC with open(schemapath) as f:
# MAGIC    d = json.load(f)
# MAGIC    schemaNew = StructType.fromJson(d)
