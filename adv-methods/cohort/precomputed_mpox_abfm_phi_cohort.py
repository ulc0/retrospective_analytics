# Databricks notebook source
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
).orderBy('note_id')

# COMMAND ----------

display(mpox_cohort)

# COMMAND ----------

mpox_cohort.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_mpox_cohort")
