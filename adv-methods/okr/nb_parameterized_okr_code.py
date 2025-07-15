# Databricks notebook source
# MAGIC %md
# MAGIC #  Intro
# MAGIC Note: The analysis uses two main tables, one where the diagnosis lives (_edav_dev_cdh.cdh_mimic_ra.fact_patient_iii_) and the NER where the snippets for entities reside (_edav_dev_cdh.cdh_mimic_exploratory.note_sentences_biomedical_ner_all_). There is an additional intermediate table (_edav_dev_cdh.cdh_mimic_ra.note_) to map _person_id_ and notes since the NER table only has _note_id_. We map everything to the patient level data
# MAGIC
# MAGIC - Accuracy: Looks at the number of patients who had at least one entity of the target condition and that is divided by the number of people who were ever diagnosed with that condition
# MAGIC
# MAGIC - Information gain: Looks for people who ever had a mention of the target condition in the NER divided by the number of people who were never diagnosed with the target condition
# MAGIC
# MAGIC Overall, there are ocations where the term is in borth the diagnosis column and the clinical note. Which in turns may reflect the lack of documentation of patient history diagnosis or the lack of connectivity between multiple EHRs 
# MAGIC
# MAGIC In most of the occasions, the target condition is in the clinical notes but it never appears in the diagnosis column. This shows an absense of documentation in EHR that this project aims to supplement. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# widget definitions
dbutils.widgets.text("experiment_id", defaultValue="846061366474489")
dbutils.widgets.dropdown(
  "catalog",
  "edav_dev_cdh",
  ["edav_dev_cdh", "edav_prd_cdh"]
)
dbutils.widgets.dropdown(
    "fact_table_schema",
    "cdh_mimic_exploratory",
    ["cdh_mimic_exploratory", "cdh_mimic_ra"],
)
dbutils.widgets.text("fact_table_name", defaultValue="fact_patient_iii")
dbutils.widgets.dropdown(
    "ner_table_schema",
    "cdh_mimic_exploratory",
    ["cdh_mimic_exploratory", "cdh_mimic_ra"],
)
dbutils.widgets.text("ner_table_name", defaultValue="note_sentences_biomedical_ner_all")
dbutils.widgets.dropdown(
    "note_table_schema",
    "cdh_mimic_exploratory",
    ["cdh_mimic_exploratory", "cdh_mimic_ra"],
)
dbutils.widgets.text("note_table_name", defaultValue="note")

# COMMAND ----------

# widget value gets
experiment_id=dbutils.widgets.get("experiment_id")
catalog=dbutils.widgets.get("catalog")
fact_table_schema=dbutils.widgets.get("fact_table_schema")
fact_table_name=dbutils.widgets.get("fact_table_name")
ner_table_schema=dbutils.widgets.get("ner_table_schema")
ner_table_name=dbutils.widgets.get("ner_table_name")
note_table_schema=dbutils.widgets.get("note_table_schema")
note_table_name=dbutils.widgets.get("note_table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
import yaml
import mlflow
import os

spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load_yaml(file_path):
    """
    Loads a YAML file and returns the parsed data as a dictionary.
    """
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data


def create_diagnosis_flag(
    df: DataFrame, diagnosis_codes, diagnosis_label, **kwargs
) -> DataFrame:
    """
    Adds a flag column for a specific diagnosis based on provided diagnosis codes

    Args:
        df (DataFrame): input dataframe
        codes:
        label:

    Returns:
        DataFrame: A new DataFrame with the diagnosis flag for the specified label
    """

    diagnosis_flag_df = (
        df.withColumn(
            diagnosis_label,
            F.when(F.col("concept_code").isin(diagnosis_codes), "1").otherwise(0),
        )
        .select("person_id", diagnosis_label)
        .groupBy("person_id")
        .agg(F.max(diagnosis_label).alias(diagnosis_label))
    )
    return diagnosis_flag_df


#TODO: replace keyword search
def filter_by_keywords(df: DataFrame, column: str, keywords: list[str]) -> DataFrame:
    """
    Filters rows in the DataFrame where the column contains any of the keywords.

    Args:
        df (DataFrame): the input DataFrame
        column (str): the column to search for keywords
        keywords (list[str]): a list of keywords to search for
    """
    condition = " or ".join([f"{column} like '%{keyword}%'" for keyword in keywords])
    filtered_df = df.where(condition)
    return filtered_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration/Experiment

# COMMAND ----------

os.environ['PYSPARK_PIN_THREAD']='False'
mlflow.autolog()

config = load_yaml("okr_config.yml")
condition = config["condition"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load/Prepare Tables for OKR Calculation

# COMMAND ----------

# contains diagnosis
fact_table = spark.table(f"{catalog}.{fact_table_schema}.{fact_table_name}")

visit_test = create_diagnosis_flag(
    fact_table,
    **condition
)

# contains patient notes
note_ra_sdf = spark.table(f"{catalog}.{note_table_schema}.{note_table_name}")

#TODO: verify the size of this table
# contains ner results from sentencized notes
note_sentences_ner = spark.table(
    f"{catalog}.{ner_table_schema}.{ner_table_name}"
)

# filter ner table for snippets containing keywords
ner_result = filter_by_keywords(
    df=note_sentences_ner, column="snippet", keywords=condition["keywords"]
)

#TODO: Implement transform chains
# see nb_abfm_notes_tagging notebook as reference
# Need to use the xref table because the NER table has only note_id,
# because I need to map instances of hypertension keyword from note
# to patient level then this join helps me to identify and aggregate that.
xref_note_ht = (
    note_ra_sdf.select("person_id", "note_id")
    .join(ner_result, ["note_id"])
    .groupBy(
        "person_id"
    )  # drop the snipet to avoid counting two distinct notes with hypertension per patient
    .count()
    .drop("count")
    .withColumn(f"{condition['diagnosis_label']}_num", F.lit(1))  # adding indicator variable for later computation
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric Calculation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy
# MAGIC
# MAGIC To calculate accuracy, need to identify instances of people who NER flags the condition (e.g. hypertension) and they were actually diagnosed ever with the condition. Then, it follows that we need to join those unique instances per person in the NER to those in the diagnosis table

# COMMAND ----------

final_table = (
    visit_test.where(f"{condition['diagnosis_label']} = 1") # diagnosis table, unique px    
    .join(
        xref_note_ht, # unique records for patient from NER
        ['person_id'],        
    )
    .fillna(0)    
)

numerator_acc = final_table.where(
    f"{condition['diagnosis_label']}_num = 1"
).count() 

denominator_acc = final_table.where(
    f"{condition['diagnosis_label']} = 1"
).count()  

acc = round((numerator_acc / denominator_acc) * 100, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log metrics to MLflow

# COMMAND ----------

#TODO: create experiment with oscar/kate
with mlflow.start_run(experiment_id=experiment_id):
    mlflow.log_metric(f"accuracy_{condition['diagnosis_label']}_percentage", acc)
