# Databricks notebook source
# MAGIC %md # Feature selection and filtering for classification

# COMMAND ----------

# MAGIC %md This notebook takes as input every table from the NER run models, reads, them and selects appropiate the features that would serve as input for classification.
# MAGIC
# MAGIC Using widgets we will read the corresponding file and use a dictionary to filter by the entity name and score for each NER output

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

dbutils.widgets.text('task_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("task_name")
print(architecture)


# COMMAND ----------

dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes")
input_table_name = dbutils.widgets.get("input_table_name")
input_table_name

# COMMAND ----------

dict_entities = {
    'biobert_diseases_ner': ['disease'],
    'bioformer_8L_ncbi_disease': ['bio'],
    'biomedical_ner_all': ['sign_symptom', 'disease_disorder'],
    'distilbert_base_uncased_ft_ncbi_disease': ['label_1', 'label_2']
}


# COMMAND ----------


model_suffix=architecture.split('/')[-1].replace('-',"_")
#model_suffix=architecture.replace('-',"_")
print(model_suffix)

# COMMAND ----------

f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_hf_{model_suffix}"

# COMMAND ----------

#odef=f"{input_table_name}_{model_suffix}"
#dbutils.widgets.text('output_table', defaultValue=f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_hf_{model_suffix}")
#output_table = dbutils.widgets.get("output_table")

#output_table = f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_hf_entity_selection_{model_suffix}"
output_table = f"edav_prd_cdh. cdh_abfm_phi_exploratory.ft_abfm_notes_hf"


print(output_table)
text_col="note_string"
output_col="results_"+model_suffix


# COMMAND ----------

output_col="results_"+model_suffix
print(output_col)

# COMMAND ----------


#adding label back for mpox diagnosed
#https://adb-5189663741904016.16.azuredatabricks.net/login.html?o=5189663741904016&next_url=%2Fjobs%2F327900031934530%2Fruns%2F528463477055578%3Fo%3D5189663741904016
initial_table = (
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_data_set_presentation_v2_run9")
    .select("patientuid","mpox_visit")
    .withColumnRenamed("patientuid", "person_id")
)

labeled_df = (
    initial_table
    .join(
        spark.table(output_table), 
        ['person_id']
        #,'left'
    )
)



# COMMAND ----------

labeled_df.select("person_id").dropDuplicates().count()

# COMMAND ----------

from pyspark.sql.functions import explode, lower

for key, value in dict_entities.items():
    if key == model_suffix:
        model_dict=dict_entities[model_suffix]
        entities_to_select = (
            labeled_df
            .withColumn(f"exploded_ent_"+model_suffix, explode(str(output_col)))
            .select("*",
                    (lower((f"exploded_ent_{model_suffix}.entity_group"))).alias(f"entity_group_{model_suffix}"),
                    (lower(f"exploded_ent_{model_suffix}.word")).alias(f"word_{model_suffix}"),
                    f"exploded_ent_{model_suffix}.score")
            .where("score > 0.95") # this may drop some patients in the cohort, think to create dummy for those but check on results first 5/2/2024
            .where(col(f"entity_group_{model_suffix}").isin(value))
    )

display(entities_to_select)

display(
    entities_to_select
    .select(f"entity_group_{model_suffix}")
    .dropDuplicates()
)

# think of droping string lenght <=2 and special characters like parenthesis and such
display(
    entities_to_select
    .select(f"word_{model_suffix}","mpox_visit")
    .groupBy(f"word_{model_suffix}","mpox_visit")    
    .count()
    .sort("mpox_visit","count", ascending = False)
)

# COMMAND ----------

# to delete 5/1/2024
output_table = f"edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf"
#edav_prd_cdh. cdh_abfm_phi_exploratory.ft_abfm_notes_hf

# COMMAND ----------

print(entities_to_select)
entities_to_select.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table+ f"_{model_suffix}")

# COMMAND ----------

output_table+f"_{model_suffix}"
