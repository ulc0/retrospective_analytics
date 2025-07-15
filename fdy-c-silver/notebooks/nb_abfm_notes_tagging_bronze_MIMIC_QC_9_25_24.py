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


# COMMAND ----------

# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = 'edav_dev_cdh'


table_options = [
    "note_silver"
]

dbutils.widgets.text("source_table", defaultValue = "note_silver")
dbutils.widgets.text("source_schema", defaultValue="cdh_ml")
dbutils.widgets.text("schema", defaultValue="cdh_ml",)
dbutils.widgets.text("table", defaultValue="aix_feedback_note_bronze")


# COMMAND ----------

catalog_name

# COMMAND ----------

args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
sink_table = f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}"

# COMMAND ----------

display(spark.table('edav_dev_cdh.cdh_ml.note_silver'))

# COMMAND ----------

f"{catalog_name}.{args['schema']}.{args['source_table']}"


# COMMAND ----------

population = spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
number_notes_to_check = 1500
word_count = 30 # proxy for tokens
population_sample = 0.1

display(population)

# COMMAND ----------

sdf_3 = (
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
  .withColumn("provider_id", F.lit(1)) 
)

def sample_patients_by_practice(df, sample_fraction=0.1):
    # Add row number for each patient within practice id
    window_spec = Window.partitionBy("provider_id").orderBy("note_id")
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
    note_to_count_w = Window.partitionBy("provider_id","note_id").orderBy(rand(seed=42)) # good enough?

    note_to_count_rn = df.select("note_id","encoding","provider_id").withColumn("note_row_num", row_number().over(note_to_count_w))
    #print("checking for 1190 (3):",note_to_count_rn.select("provider_id").distinct().count())
    note_to_count_rn = note_to_count_rn.drop("provider_id")

    notes_filtered = note_to_count_rn.filter(F.col("note_row_num") <= 5).drop('encoding')#.drop("note_row_num")    
    #print("checking for 1190 (4):",note_to_count_rn.select("provider_id").distinct().count())
    
    # Drop unnecessary columns
    #df_result = df_sampled.join(notes_filtered, on=["person_id", "note_id",'provider_id'], how = 'left').drop("row_num", "count", "sample_size")
    df_result = df_sampled.join(notes_filtered, on=['note_id']).drop("row_num", "count", "sample_size")
    
    return df_result

# Apply the function to sample 10% of users per business

#print("checking for 1190 (5):", result_df_0.select("provider_id").distinct().count())
result_df_3 = sample_patients_by_practice(sdf_3, sample_fraction=0.1)


# COMMAND ----------

result_df_3.cache()

# COMMAND ----------

result_df_3 = (    
    result_df_3
    .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
    .withColumn("word_30_or_more", F.when(F.col("wordCount")> word_count, 1).otherwise(0))
)

# COMMAND ----------

display(result_df_3.select('encoding').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at number of practices with notes whose lenght is greater than 30 words

# COMMAND ----------


display(result_df_3)

print("note_id:",result_df_3.where("word_30_or_more == 1").select('note_id').distinct().count())
print("provider_id:",result_df_3.where("word_30_or_more == 1").select('provider_id').distinct().count())

# COMMAND ----------

result_df_3 = result_df_3.where("word_30_or_more == 1").orderBy("note_id")

df_spf_utf = (
    result_df_3
    .where("encoding = 'utf'")    
    .limit(number_notes_to_check)
)

df_spf_rtf = (
    result_df_3
    .where("encoding = 'rtf'")    
    .limit(number_notes_to_check)
)

df_spf_xml = (
    result_df_3
    .where("encoding = 'xml'")    
    .limit(number_notes_to_check)
)

df_spf_htm = (
    result_df_3
    .where("encoding = 'htm'")    
    .limit(number_notes_to_check)
)

df_spf_sop = (
    result_df_3
    .where("encoding = 'sop'")    
    .limit(number_notes_to_check)
)

sdf_union_3 = (
    df_spf_utf
    .unionByName(df_spf_rtf)
    .unionByName(df_spf_xml)
    .unionByName(df_spf_htm)
    .unionByName(df_spf_sop)     
    .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
    .withColumn('words', F.split(F.col("clean_text"),' '))      
    .select('encoding',"clean_text","wordCount","provider_id", "words", "note_id")
)

sdf_union_3.cache()
display(sdf_union_3)

display(
    sdf_union_3
    .where("encoding = 'rtf'")
    )

print(sdf_union_3.count())

# COMMAND ----------

display(
    sdf_union_3
    .groupBy("encoding","provider_id")
    .count()
    .orderBy("count", ascending = False)
)



# COMMAND ----------

sdf_union_3.count()

# COMMAND ----------

# MAGIC %md ### Checking utf and markup patterns

# COMMAND ----------

# MAGIC %md 
# MAGIC In this section we revised some markup patterns that are still lingering and that can flag possible additional cleaning cases. We do this manually, then we use the identified cases in the 10% sample to find frequency

# COMMAND ----------

## make one with the aggregate data, just to see most common pattern. The limit(1500) seems to be random
sdf_union_3_markup = (
  sdf_union_3
  .withColumn("truncated_text",F.expr("array_join(slice(words, 1, least(size(words), 50)), ' ')"))
  .where("""
         clean_text like '%b>' 
         or clean_text like '%u>' 
         or clean_text like '%div>' 
         or  clean_text like '%xlm' 
         or clean_text like '%>'
         or clean_text like '%tr>'
         or clean_text like '%<b'
         """)
)

display(sdf_union_3_markup)

display(
  sdf_union_3_markup
  .groupBy("provider_id","encoding")
  .count()
)


## select for the truncated text to find the pattens, notes tend to be too long and I cannot see all the rows.

# COMMAND ----------

display(
    sdf_union_3_markup
    .where("encoding == 'utf' and provider_id in ('1639', '1624', '1511')")
    )
 
display(
    sdf_union_3_markup    
    .where("encoding == 'htm'")
    )

## issues at the end of the cleaning, not too concerning most common pattern observed is something like "<div align", "" <span style" etc. at the very end of the sentence


# COMMAND ----------

display(
    sdf_union_3
    .where("encoding = 'htm'")
    .withColumn("truncated_text",F.expr("array_join(slice(words, 1, least(size(words), 50)), ' ')"))
)

display(
    sdf_union_3
    .where("encoding = 'rtf'")    
)


display(
    sdf_union_3
    .where("encoding = 'rtf'")
    .withColumn("truncated_text",F.expr("array_join(slice(words, 1, least(size(words), 50)), ' ')"))
    .drop('clean_text','words','note_text') # drpopping these cols because rtf notes tend to be larger, so for the display I cannot see many
)

# COMMAND ----------

# MAGIC %md Now, with the identified patten cases we looked at the full sample of the data to identify in which practices this case repeats the most

# COMMAND ----------

result_df_3

## make one with the aggregate data, just to see most common pattern. The limit(1500) seems to be random
sdf_sample = (
  result_df_3  
  .where("""
         clean_text like '%b>' 
         or clean_text like '%u>' 
         or clean_text like '%div>' 
         or clean_text like '%xlm' 
         or clean_text like '%>'
         or clean_text like '%tr>'
         or clean_text like '%<b'
         
         """)
)

display(sdf_sample)

numerator = (
  sdf_sample
  .groupBy("provider_id","encoding")
  .count()
  .orderBy("count", ascending = False)
)



# COMMAND ----------

sdf_sample = (
  result_df_3  
  .where("""
         clean_text like '%%'                   
         """)
)

display(sdf_sample)

## note id non-printable: 58396, 25769864346, 34359796601

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary 

# COMMAND ----------


denominator = (
    result_df_3
    .groupBy("provider_id","encoding")
    .count()
    .orderBy("count", ascending = False)
)

summary = ( 
           numerator
           .join(
               denominator.withColumnRenamed("count", "denominator")
               , ['provider_id',"encoding"]
               )
           .withColumn("proportion_cases_perc", 
                       F.round((F.col("count")/F.col("denominator"))*100.0,2)
                       )
           .orderBy("proportion_cases_perc", ascending = False)
)

display(summary)

# COMMAND ----------

# MAGIC %md when looking at the full sample and the identified pattern there the overlap

# COMMAND ----------

sdf_3 = (
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
  .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
  .withColumn('words', F.split(F.col("clean_text"),' '))  

)

print('Quick summary - word count')

display(
    sdf_3
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)


print('Quick summary - word count >= 30')
display(
    sdf_3
    .where("wordCount>=30")
    .select('wordCount')
    .summary('count',"min","25%","50%","75%","max")
)


# COMMAND ----------

# count all record
sdf_3.count()

# COMMAND ----------

# switch to regular python to create this

print("Distribution of notes >= 30 words")
display(
    sdf_3
    .where("wordCount>=30")
    .groupby('encoding','wordCount')
    .count()
)

# COMMAND ----------

display(
    sdf_3
    .where("wordCount>=30")    
    .groupby('encoding','wordCount')
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md ## Test cases information

# COMMAND ----------

display(
    result_df_3
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Looking at cleaned table 9/21/2024

# COMMAND ----------

result_df_3_test = (
    #spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver@v3")
    spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
    .select("note_id","clean_text")
)

display(result_df_3_test)
