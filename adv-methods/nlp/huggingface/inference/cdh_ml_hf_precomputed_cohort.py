# Databricks notebook source
dbutils.widgets.text('bsize',defaultValue='1',label="Training Batch Size")
bsize=int(dbutils.widgets.get('bsize'))

# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------

#dbutils.widgets.text('hftask'  , defaultValue="token-classification")
#hftask = dbutils.widgets.get("hftask")

# COMMAND ----------

dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze")
input_table_name = dbutils.widgets.get("input_table_name")
dbutils.widgets.text('text_col' , defaultValue="clean_text")
#text_col="note_text"
text_col = dbutils.widgets.get("text_col")

# COMMAND ----------

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------



odef=f"{input_table_name}_mpox"
dbutils.widgets.text('output_table', defaultValue=odef )
output_table = dbutils.widgets.get("output_table")
print(output_table)



# COMMAND ----------

mpox_cohort = (
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_nlp_mpox_notes") # cohort of interest, includes labels
    .withColumnRenamed("patientuid", "person_id")
    .withColumnRenamed("exposed","label")
    .select("person_id",'label')
    .dropDuplicates()
    .join(        
        spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_bronze")
        .select(
            'person_id',
            "note_id",      
            )
        ,['person_id']
    )    
)

# COMMAND ----------

df = spark.table(input_table_name).select(['note_id',text_col,]).join(mpox_cohort, ['note_id'])#.limit(1000)

# COMMAND ----------

#display(df)

# COMMAND ----------

output_table

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

# COMMAND ----------

#edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox

# COMMAND ----------

# node id for testing scispacy
import pyspark.sql.functions as F

display(
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox_biobert_diseases_ner")
    #.filter(F.length(F.col("snippet")) > 30)
)

# COMMAND ----------

note_id in ('25314537301941', '75325136836402', '3719441720598','56496000087007','56496000087007','75325136836402')
