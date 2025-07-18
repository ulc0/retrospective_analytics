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

#/Workspace/Repos/run9@cdc.gov/cdh-featurization/notebooks

# COMMAND ----------

# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")

# mandatory parameters and names. The orchestrator will always pass these

table_options = [
    "note_silver"
]

dbutils.widgets.text("source_table", defaultValue = "aix_feedback_note_bronze")
dbutils.widgets.text("source_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("schema", defaultValue="cdh_abfm_phi_exploratory",)
dbutils.widgets.text("table", defaultValue="aix_feedback_note_bronze")


# COMMAND ----------

catalog_name

# COMMAND ----------

args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
#cohort_table = f"{catalog_name}.{args['schema']}.{args['cohort_table']}"
sink_table = f"{catalog_name}.{args['schema']}.aix_demo_{args['source_table']}"

# COMMAND ----------

f"{catalog_name}.{args['schema']}.{args['source_table']}"

# COMMAND ----------

population = spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
number_notes_to_check = 1500
word_count = 30 # proxy for tokens
population_sample = 0.1

# COMMAND ----------

print(population.select("provider_id").where("person_id = ' ' ").count())

display(population.groupBy("provider_id").count())

# COMMAND ----------


sdf_3 = (
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}") 
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

#print("checking for 1190 (5):", result_df_0.select("provider_id").distinct().count())
result_df_3 = sample_patients_by_practice(sdf_3, sample_fraction=0.1)




# COMMAND ----------

missing_providers = (
    population
    .select("provider_id")
    .distinct()
    .subtract(
        result_df_3
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
    .where("provider_id in (104)")
    )

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_QC_9_23_24""")

# COMMAND ----------

result_df_3.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_9_23_24")

# COMMAND ----------

result_df_3 = (
    #spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver@v3")
    spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_9_23_24")
    .withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
    .withColumn("word_30_or_more", F.when(F.col("wordCount")> word_count, 1).otherwise(0))
)

# COMMAND ----------

f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_9_23_24"

# COMMAND ----------

display(result_df_3.select('encoding').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at number of practices with notes whose lenght is greater than 30 words

# COMMAND ----------


display(result_df_3)
print("person_id:",result_df_3.where("word_30_or_more == 1").select('person_id').distinct().count())
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
    .select('person_id',"note_text",'encoding',"clean_text","wordCount","provider_id", "words", "note_id")
)

sdf_union_3.cache()
display(sdf_union_3)

display(
    sdf_union_3
    .where("encoding = 'rtf'")
    )

print(sdf_union_3.count())

# COMMAND ----------

# MAGIC %md
# MAGIC b> or u> or div>

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

# MAGIC %md ### looking at changes with respect to previous run per provider_id

# COMMAND ----------

display(
    sdf_sample
    .where("""provider_id in (71,48)""")
)

display(
    sdf_sample
    .where("""provider_id in (908, 909, 914)""")
)


# xlm s: 54107998378894, 82609401181045, 89678917543012

# malformed xlm: 67997922668980, 15522011834113

# COMMAND ----------

display(
    sdf_sample
    .where("""provider_id in (941, 947, 987)""")
)

display(
    sdf_sample
    .where("""provider_id in (992, 1031, 1050, 1059, 1167, 1172)""")
)

# 30236569792991, 2954937528788, 34273839050941, 108800111757331, 112382114303306

# COMMAND ----------


display(
    sdf_sample
    .where("""provider_id in (1184, 1201, 1210, 1225)""")
)

display(
    sdf_sample
    .where("""provider_id in (1249, 1251, 1264)""")
)

#197568527897, 3633542364311, 9311489129585, 31301721686465, 32624571612859, 34471407550352

# COMMAND ----------


display(
    sdf_sample
    .where("""provider_id in (1301, 1308, 1310, 1314)""")
)

display(
    sdf_sample
    .where("""provider_id in (1340, 1370, 1426, 1452)""")
)

#

# COMMAND ----------


display(
    sdf_sample
    .where("""provider_id in (1454, 1471, 1510, 1529)""")
)

display(
    sdf_sample
    .where("""provider_id in (1454, 1471, 1510, 1529, 1530)""")
)

# 60129576389, 369367221096, 17197049086455, 214748399689, 3865470600220

# COMMAND ----------

display(
    sdf_sample
    .where("""provider_id in (1639, 1723, 1729, 1753, 1754)""")
)

display(
    sdf_sample
    .where("""provider_id in (2339, 2405, 1840)""")
)

#7859790187666, 12017318530663, 20263655738961, 114254720014135, 116531052851634

# COMMAND ----------

# MAGIC %md ## looking at some sample from the denominator

# COMMAND ----------

display( 
    result_df_3
    .select('note_id','clean_text')
)

#26157, 30247, 35986, 17179900724, 17179910766

# COMMAND ----------

# MAGIC %md 
# MAGIC # Observations run 9_23_24
# MAGIC
# MAGIC ## Overall
# MAGIC It seems like the encoding tagging fails sometimes in classifing a note with the correct encoding. More generally xml fails to be classified correctly and becomes utf. More cases were observed for provider_id: '2405','2339', and '314'. 
# MAGIC ## rft
# MAGIC The rtf lools good, I think cleaning for the non-printable characters would be ideal but it is looking good. 
# MAGIC
# MAGIC
# MAGIC ## utf-8
# MAGIC In general utf seems good, the issue is classification of some notes that have markup insight the text as mentioned in the previous iteration.
# MAGIC
# MAGIC ## html
# MAGIC
# MAGIC In general looks good, same issue with the utf
# MAGIC
# MAGIC ##xlm
# MAGIC
# MAGIC In general looks good, same issue with the utf
# MAGIC
# MAGIC

# COMMAND ----------

sdf_3 = (
  #spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver@v3")
  spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}_QC_9_9_24") 
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
    .select('person_id','note_id','encoding','note_datetime','note_type','note_title','note_text','clean_text')
    .where("""
        note_id in (
            26157, 30247, 35986, 17179900724, 17179910766, 7859790187666, 12017318530663, 20263655738961, 114254720014135, 116531052851634, 60129576389, 369367221096, 17197049086455, 214748399689, 3865470600220, 197568527897, 3633542364311, 9311489129585, 31301721686465, 32624571612859, 34471407550352, 30236569792991, 2954937528788, 34273839050941, 108800111757331, 112382114303306, 67997922668980, 15522011834113, 54107998378894, 82609401181045, 89678917543012
        )
    """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Looking at cleaned table 9/21/2024

# COMMAND ----------

result_df_3_test = (
    #spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver@v3")
    spark.table(f"{catalog_name}.{args['schema']}.{args['source_table']}")
    .select("note_id","note_text","clean_text")
    #.withColumn('wordCount', F.size(F.split(F.col("clean_text"),' ')))
    #.withColumn("word_30_or_more", F.when(F.col("wordCount")> word_count, 1).otherwise(0))
    .where(
        """
        note_id in (
            26157, 30247, 35986, 17179900724, 17179910766, 7859790187666, 12017318530663, 20263655738961, 114254720014135, 116531052851634, 60129576389, 369367221096, 17197049086455, 214748399689, 3865470600220, 197568527897, 3633542364311, 9311489129585, 31301721686465, 32624571612859, 34471407550352, 30236569792991, 2954937528788, 34273839050941, 108800111757331, 112382114303306, 67997922668980, 15522011834113, 54107998378894, 82609401181045, 89678917543012
        )
    """

    )
)

display(result_df_3_test)
