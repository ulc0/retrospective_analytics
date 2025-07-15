# Databricks notebook source
# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC load data local infile 'RXNATOMARCHIVE.RRF' into table RXNATOMARCHIVE fields terminated by '|' 
# MAGIC ESCAPED BY '�'
# MAGIC lines terminated by '\n'
# MAGIC (@rxaui,@aui,@str,@archive_timestamp,@created_timestamp,@updated_timestamp,@code,@is_brand,@lat,@last_released,@saui,@vsab,@rxcui,@sab,@tty,@merged_to_rxcui)
# MAGIC SET rxaui =@rxaui,
# MAGIC     aui =@aui,
# MAGIC     str =@str,
# MAGIC     archive_timestamp =@archive_timestamp,
# MAGIC     created_timestamp =@created_timestamp,
# MAGIC     updated_timestamp =@updated_timestamp,
# MAGIC     code =@code,
# MAGIC     is_brand =@is_brand,
# MAGIC     lat =@lat,
# MAGIC     last_released =@last_released,
# MAGIC     saui =@saui,
# MAGIC     vsab =@vsab,
# MAGIC     rxcui =@rxcui,
# MAGIC     sab =@sab,
# MAGIC     tty =@tty,
# MAGIC     merged_to_rxcui =@merged_to_rxcui;
# MAGIC
# MAGIC ```

# COMMAND ----------

# Imports
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType

# COMMAND ----------

sch={}
sch["rxnconso"] = StructType() \
      .add("rxcui",IntegerType(),True) \
      .add("lat",StringType(),True) \
      .add("ts",StringType(),True) \
      .add("lui",IntegerType(),True) \
      .add("stt",StringType(),True) \
      .add("sui",IntegerType(),True) \
      .add("ispref",StringType(),True) \
      .add("rxaui",IntegerType(),True) \
      .add("saui",IntegerType(),True) \
      .add("scui",IntegerType(),True) \
      .add("sdui",IntegerType(),True) \
      .add("sab",StringType(),True) \
      .add("tty",StringType(),True) \
      .add("code",IntegerType(),True) \
      .add("str",StringType(),True) \
      .add("srl",StringType(),True) \
      .add("supress",StringType(),True) \
      .add("cvf",StringType(),True) 


# COMMAND ----------

ignore={}
ignore["rxnconso"]=["lat","ts","lui","stt","sui","ispref","srl","cvf"]

# COMMAND ----------

def rffRead(rrf,schemaEntry):
    df=spark.read.option("sep","|").option("escape","�").option("lineSep",'\n').schema(schemaEntry).csv(rrf)#.drop(*ignore[schemaEntry])
    #df.printSchema()
    return(df)


# COMMAND ----------

fn="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/rrf/RXNCONSO.RRF"
rx=rffRead(fn,sch["rxnconso"]).drop(*ignore["rxnconso"])

# COMMAND ----------

display(rx)
