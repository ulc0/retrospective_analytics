# Databricks notebook source
# MAGIC %md
# MAGIC ```sql
# MAGIC select distinct *
# MAGIC (
# MAGIC     SELECT distinct raw_concept_code as concept, src_vocabulary_id,src_domain_id,src_concept_id,tar_concept_id FROM edav_prd_cdh.cdh_premier_omop.concept_condition
# MAGIC union
# MAGIC SELECT clean_concept_code as concept, src_vocabulary_id,src_domain_id,src_concept_id,tar_concept_id FROM edav_prd_cdh.cdh_premier_omop.concept_condition
# MAGIC )
# MAGIC order by concept, src_vocabulary_id
# MAGIC
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce
import pyspark.sql.types as T 

# COMMAND ----------

cols=['src_vocabulary_id','src_domain_id','src_concept_id','tar_concept_id',]
concept_cols=['raw_concept_code','clean_concept_code',]
concept_table='condition'

# COMMAND ----------

omop_concept_df=spark.table(f"edav_prd_cdh.cdh_premier_omop.concept_{concept_table}")\
    .withColumn('src_concept_id',F.col('src_concept_id').cast("long"))\
    .withColumn('tar_concept_id',F.col('tar_concept_id').cast("long"))
<<<<<<< HEAD

new_concept_tbl=f"edav_prd_cdh.cdh_ml.omop_concept_{concept_table}"

=======
new_concept_tbl=f"edav_prd_cdh.cdh_ml.omop_concept_{concept_table}"
>>>>>>> b1c88a5 (update)

# COMMAND ----------

#create empty struct from abbreviated schema
old_concept_cols=['raw_concept_code','clean_concept_code']
concept_cols=omop_concept_df.drop(old_concept_cols[0]).columns[:5]
print(concept_cols)
concept_schema=omop_concept_df.select(*concept_cols).withColumnRenamed(old_concept_cols[1],'concept_code').schema
print(concept_schema)
#concept_col=spark.createDataFrame([],concept_schema)

# COMMAND ----------

cdf_list=[]
for concept in old_concept_cols:
    select_cols=[concept]+cols
    print(select_cols)
    cdf=omop_concept_df.select(select_cols).withColumnRenamed(concept,'concept_code')
    display(cdf)
    cdf_list.append(cdf)
#    display(df_list)
#    new_concept_df=new_concept_df.union(cdf)
#    display(new_concept_df)


# COMMAND ----------

concept_df=reduce(lambda df1, df2: df1.union(df2), cdf_list).orderBy("concept_code").dropDuplicates() #.withColumnsRenamed(renameDict)
display(concept_df)

# COMMAND ----------

concept_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(new_concept_tbl) 
#.partitionBy("vocabulary_id").saveAsTable(f"{fact_df}")

