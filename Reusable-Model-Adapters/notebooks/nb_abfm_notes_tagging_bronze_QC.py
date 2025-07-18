# Databricks notebook source
# MAGIC %md 
# MAGIC # Objective
# MAGIC
# MAGIC - To serve as QC for output results and identify what are could be some high value low effort changes that can improve notes readability
# MAGIC - This notebook looks at a sample of the whole ABFM data set to identofy additional markup clean up and extraction requirements if any
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rand


from delta.tables import *

deltaTable = DeltaTable.forName(spark, "main.default.people_10m")
deltaTable.optimize()

# COMMAND ----------

# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")

# mandatory parameters and names. The orchestrator will always pass these

table_options = [
    "note_silver"
]

dbutils.widgets.dropdown("source_table", "note_silver", table_options)
dbutils.widgets.text("source_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("schema", defaultValue="cdh_abfm_phi_exploratory",)
dbutils.widgets.text("cohort_table", defaultValue="mpox_data_set_presentation_v2_run9")


# COMMAND ----------

args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
cohort_table = f"{catalog_name}.{args['schema']}.{args['cohort_table']}"
sink_table = f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}"

# COMMAND ----------

f"{catalog_name}.{args['schema']}.{args['source_table']}"

# COMMAND ----------

population = spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}@v0")
number_notes_to_check = 1500
word_count = 30 # proxy for tokens
population_sample = 0.1

# COMMAND ----------

print(population.select("provider_id").where("person_id = ' ' ").count())

display(population.groupBy("provider_id").count())

# COMMAND ----------


sdf_0 = (
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}@v0") 
)

sdf_3 = (
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}@v3") 
)

def sample_patients_by_practice(df, sample_fraction=0.1):
    # Add row number for each patient within practice id
    window_spec = Window.partitionBy("provider_id").orderBy("person_id")
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    
    df_counts = df.groupBy("provider_id").count()
    
    # Calculate the number of users to sample 
    df_with_counts = df_counts.join(df_with_row_num, on="provider_id", how = 'left')
    df_with_counts = df_with_counts.withColumn("sample_size", (F.col("count") * sample_fraction).cast("int"))
    #print("checking for 1190:",df_with_counts.select("provider_id").distinct().count())

    # Ensure at least one user is sampled per business if there are users
    df_with_counts = df_with_counts.withColumn("sample_size",  F.when(F.col("sample_size") > 0, F.col("sample_size")).otherwise(F.lit(1)))
    
    # Filter users based on sample size
    df_sampled = df_with_counts.filter(F.col("row_num") <= F.col("sample_size"))
    #print("checking for 1190 (2):",df_sampled.select("provider_id").distinct().count())

    # creating window per note patient, up to 5. Using a 'new' dataframe
    #note_to_count_w = Window.partitionBy("person_id").orderBy("provider_id","encoding", rand(seed=42)) # ideal but expensive
    #note_to_count_w = Window.partitionBy("person_id","provider_id").orderBy("encoding", rand(seed=42)) # ideal but expensive
    note_to_count_w = Window.partitionBy("provider_id","person_id").orderBy(rand(seed=42)) # good enough?

    note_to_count_rn = df.select("person_id","note_id","encoding","provider_id").withColumn("note_row_num", row_number().over(note_to_count_w))
    #print("checking for 1190 (3):",note_to_count_rn.select("provider_id").distinct().count())
    note_to_count_rn = note_to_count_rn.drop("provider_id")

    notes_filtered = note_to_count_rn.filter(F.col("note_row_num") <= 5).drop('encoding')#.drop("note_row_num")    
    #print("checking for 1190 (4):",note_to_count_rn.select("provider_id").distinct().count())
    
    # Drop unnecessary columns
    #df_result = df_sampled.join(notes_filtered, on=["person_id", "note_id",'provider_id'], how = 'left').drop("row_num", "count", "sample_size")
    df_result = df_sampled.join(notes_filtered, on=["person_id",'note_id']).drop("row_num", "count", "sample_size")
    
    return df_result

# Apply the function to sample 10% of users per business
result_df_0 = sample_patients_by_practice(sdf_0, sample_fraction=0.1)
#print("checking for 1190 (5):", result_df_0.select("provider_id").distinct().count())
result_df_3 = sample_patients_by_practice(sdf_3, sample_fraction=0.1)

#display(result_df)
#result_df_0.count()

# COMMAND ----------

f"{catalog_name}.{args['schema']}.{args['source_table']}@v3"

# COMMAND ----------

missing_providers = (
    population
    .select("provider_id")
    .distinct()
    .subtract(
        test
        .where("note_row_num is not null")
        .select("provider_id")
        .distinct()
    )
)

display(missing_providers)


# COMMAND ----------

display(
    population
    .where("provider_id in (254)")
    )

display(
    population
    #.select("person_id","provider_id","encoding")
    #.distinct()
    .where("provider_id in (254)")
    )

# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_QC_v0""")

# COMMAND ----------

#result_df_0.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_v0")

# COMMAND ----------

#result_df_0 = spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_v0")

# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_QC_v3""")

# COMMAND ----------

#result_df_3.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_v3")

# COMMAND ----------

result_df_3 = spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_v3")

# COMMAND ----------

display(result_df_3.select('encoding').distinct())

# COMMAND ----------

display(result_df_3)
print("person_id:",result_df_3.select('person_id').distinct().count())
print("note_id:",result_df_3.select('note_id').distinct().count())
print("provider_id:",result_df_3.select('provider_id').distinct().count())

# COMMAND ----------


df_spf_utf = (
    result_df_3
    .where("encoding = 'utf'")    
    .limit(1500)
)

df_spf_rtf = (
    result_df_3
    .where("encoding = 'rtf'")    
    .limit(1500)
)

df_spf_xml = (
    result_df_3
    .where("encoding = 'xml'")    
    .limit(1500)
)

df_spf_htm = (
    result_df_3
    .where("encoding = 'htm'")    
    .limit(1500)
)

df_spf_sop = (
    result_df_3
    .where("encoding = 'sop'")    
    .limit(1500)
)

sdf_union_3 = (
    df_spf_utf
    .unionByName(df_spf_rtf)
    .unionByName(df_spf_xml)
    .unionByName(df_spf_htm)
    .unionByName(df_spf_sop)
    .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
    .withColumn('words', F.split(F.col("clean_text"),' '))  
    .select('person_id',"note_text",'encoding',"clean_text")
)

sdf_union_3.cache()
display(sdf_union_3)



# COMMAND ----------

display(
    result_df_3
    .select('encoding')
    .distinct()
)

# COMMAND ----------

# total population: 11222439222 --- 11 billion rows
# sample = 0.1:      1122165102 --- 1.1 billon rows


# COMMAND ----------

# MAGIC %md 
# MAGIC # Initial observations 
# MAGIC
# MAGIC ## Overall
# MAGIC I believe most clean notes are human readable, there are some instances mainly in the utf-8 where there is markup in the middle of the note that woud need a bit more processing. For the other formats, cleaning some special characters may be useful. I do not know to what extend it would affect the NER if not cleaned. 
# MAGIC
# MAGIC ## rft
# MAGIC - The latest version of rtf looks good, a couple of things that can improve are the reduction of pipes "|" and or consecutive underscores "____________". Consider replacing with something that is a better suited separator like a period, semicolon or something that is more frequently used for NLP task. Also, there are some instances where text have these character   (see person_id in ('D59B65AD-9302-4FF6-AB81-402D606FDD11', 'DD8ABC9D-966A-4B10-9F75-A6FE97A66455')), seems like a separator as well.
# MAGIC
# MAGIC ## utf-8
# MAGIC - Some notes may have markup in the middle (see person_id in ('82586d2a-8f90-49b6-9040-0c28550e0227', '218ea43f-0051-4a51-bbb3-37659f1d46ce','87189281-F39B-4DDD-B302-0B57721CC930')), I believe they are html (?) and pipes are also present in some instances
# MAGIC - Some notes may have consecutive sequence of characters before  the start of a word/ sentence. For example "_, _,". (see person_id = '2538dcce-837a-4f93-a9f4-efe33ef925b2')
# MAGIC
# MAGIC ## html
# MAGIC - Overall it looks good, some files may end with some markup traces like person_id = '82586d2a-8f90-49b6-9040-0c28550e0227'. Note that this cohort does not contain many different DISTINCT person_id. Need to run additional test on larger sample
# MAGIC
# MAGIC ##xlm
# MAGIC
# MAGIC - When exploring the results there are some pattern removal that could be removed like ";&#160;&#160;&#160;&#160;&#160;&#160;&#160'" it is like a special character for non-breaking space (see https://en.wikipedia.org/wiki/Non-breaking_space)  for person_id = '5db00c20-5237-4036-8bb3-a4044fa512fa'. This is an intance but there are multiple like this, and it seems to be towards the end of the note. May be related to finalizing a note 

# COMMAND ----------

# MAGIC %md 
# MAGIC # Creating some stats

# COMMAND ----------

sdf = (
  spark.table(f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}")
  .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
  .withColumn('words', F.split(F.col("clean_text"),' '))  

)


print('Quick summary - word count')
display(
    sdf
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)


print('Quick summary - word count >= 30')
display(
    sdf
    .where("wordCount>=30")
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)

# COMMAND ----------

# count all record
sdf.count()

# COMMAND ----------

# switch to regular python to create this

print("Distribution of notes >= 30 words")
display(
    sdf
    .where("wordCount>=30")
    .select('encoding','wordCount')
)

# COMMAND ----------

display(
    sdf
    .where("wordCount>=30")
    .select('encoding','wordCount')
    .sort("wordCount")
)

# COMMAND ----------

f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}"

# COMMAND ----------

#patientnotepreoblem

#These results are for this specific cohort, which is a subset of the whole data set.
sdf = (
  spark.table(f"edav_prd_cdh.cdh_abfm_phi_exploratory.aix_demo_patientnoteresultobservation_ml")
  .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
  .withColumn('words', F.split(F.col("clean_text"),' '))
  
)

df_spf = (
  sdf
  .where('wordCount >= 30') # proxy for minimun note lenght
  .withColumn('row_num', F.row_number().over(Window.partitionBy("encoding").orderBy(F.rand(seed = 42))))
  .withColumn("trucated_text", F.expr("array_join(slice(words, 1, least(size(words), 50)), ' ')"))
  .filter(F.col('row_num') <= 150) # checking 150 notes of at least 30 words long
  .select('person_id','row_num',"note_text",'encoding',"clean_text","trucated_text")
  
)


print('Quick summary')
display(
  df_spf
  .groupBy('encoding')  
  .agg(
    F.collect_list("trucated_text").alias("trucated_text"),
  )
)

print('rtf format')
display(df_spf.where("encoding = 'rtf'"))
print('utf-8 format')
display(df_spf.where("encoding = 'utf'"))
print('xlm format')
display(df_spf.where("encoding = 'xml'"))
print('html format')
display(df_spf.where("encoding = 'html'"))


# COMMAND ----------

# MAGIC %md
# MAGIC patientresultobservation xlm and html have the &#160 in both tables in this cohort

# COMMAND ----------

sdf = (
  spark.table(f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}")
  .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
  .withColumn('words', F.split(F.col("clean_text"),' '))  

)


print('Quick summary - word count')
display(
    sdf
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)


print('Quick summary - word count >= 30')
display(
    sdf
    .where("wordCount>=30")
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)

# COMMAND ----------


