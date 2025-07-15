# Databricks notebook source
# mandatory parameters and names. The orchestrator will always pass these

dbutils.widgets.text("notetable", defaultValue="bronze_note") 
dbutils.widgets.text("src_catalog", defaultValue="edav_prd_cdh")
dbutils.widgets.text("src_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("catalog",defaultValue="edav_prd_cdh",)
dbutils.widgets.text("schema",defaultValue="cdh_abfm_phi_exploratory",)
dbutils.widgets.text("experiment_id", defaultValue="3389456715448166")
dbutils.widgets.text("output_table", defaultValue="")

# COMMAND ----------

#cohortTbl = dbutils.widgets.get("cohorttbl")
#LIMIT = dbutils.widgets.get("LIMIT")
EXPERIMENT_ID=dbutils.widgets.get("experiment_id")
src_schema = dbutils.widgets.get("src_schema")
schema = dbutils.widgets.get("schema")
src_catalog = dbutils.widgets.get("src_catalog")
catalog = dbutils.widgets.get("catalog")
print(catalog)
notetable = dbutils.widgets.get("notetable")
print(notetable)
output_table=dbutils.widgets.get("output_table")
print(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC [note](https://ohdsi.github.io/CommonDataModel/cdm54.html#note) Table Description  
# MAGIC
# MAGIC The NOTE table captures unstructured information that was recorded by a provider about a patient in free text (in ASCII, or preferably in UTF8 format) notes on a given date. The type of note_text is CLOB or varchar(MAX) depending on RDBMS.
# MAGIC
# MAGIC User Guide  
# MAGIC
# MAGIC NA  
# MAGIC
# MAGIC ETL Conventions  
# MAGIC
# MAGIC HL7/LOINC CDO is a standard for consistent naming of documents to support a range of use cases: retrieval, organization, display, and exchange. It guides the creation of LOINC codes for clinical notes. CDO annotates each document with 5 dimensions:  
# MAGIC
# MAGIC * **Kind of Document**: Characterizes the general structure of the document at a macro level (e.g. Anesthesia Consent)  
# MAGIC * **Type of Service**: Characterizes the kind of service or activity (e.g. evaluations, consultations, and summaries). The notion of time sequence, e.g., at the beginning (admission) at the end (discharge) is subsumed in this axis. Example: Discharge Teaching.  
# MAGIC * **Setting**: Setting is an extension of CMS’s definitions (e.g. Inpatient, Outpatient)  
# MAGIC * **Subject Matter Domain (SMD)**: Characterizes the subject matter domain of a note (e.g. Anesthesiology)  
# MAGIC * **Role**: Characterizes the training or professional level of the author of the document, but does not break down to specialty or subspecialty (e.g. Physician) Each combination of these 5 dimensions rolls up to a unique LOINC code.  
# MAGIC According to CDO requirements, only 2 of the 5 dimensions are required to properly annotate a document; Kind of Document and any one of the other 4 dimensions. However, not all the permutations of the CDO dimensions will necessarily yield an existing LOINC code. Each of these dimensions are contained in the OMOP Vocabulary under the domain of ‘Meas Value’ with each dimension represented as a Concept Class.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC [OMOP CDM 5.4 NOTE table](https://ohdsi.github.io/CommonDataModel/cdm54.html#note)  drop _concept_id so no link to concepts, where do we assign note_id? Bronze  
# MAGIC
# MAGIC | **<br>CDM Field<br>**                   | **<br>User Guide<br>**                                                                                                                                                                  | **<br>ETL Conventions<br>**                                                                                                                                                                                                                                                                      | **<br>Datatype<br>** | **<br>Required<br>** | **<br>Primary Key<br>** | **<br>Foreign Key<br>** | **<br>FK Table<br>**     | **<br>FK Domain<br>** |
# MAGIC |-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|----------------------|-------------------------|-------------------------|--------------------------|-----------------------|
# MAGIC | **<br>note_id<br>**                     | <br>A unique identifier for each note.<br>                                                                                                                                              | <br>                                                                                                                                                                                                                                                                                             | <br>integer<br>      | <br>Yes<br>          | <br>Yes<br>             | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | **<br>person_id<br>**                   | <br>                                                                                                                                                                                    | <br>                                                                                                                                                                                                                                                                                             | <br>integer<br>      | <br>Yes<br>          | <br>No<br>              | <br>Yes<br>             | <br>PERSON<br>           | <br>                  |
# MAGIC | **<br>note_date<br>**                   | <br>The date the note was recorded.<br>                                                                                                                                                 | <br>                                                                                                                                                                                                                                                                                             | <br>date<br>         | <br>Yes<br>          | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | **<br>note_datetime<br>**               | <br>                                                                                                                                                                                    | <br>If time is not given set the time to midnight.<br>                                                                                                                                                                                                                                           | <br>datetime<br>     | <br>No<br>           | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | **<br>note_type<br>**        | <br>The provenance of the note. Most likely this will be EHR.<br>                                                                                                                       | <br>Put the source system of the note, as in EHR record. Accepted<br>Concepts. A more detailed explanation of each Type Concept can be<br>found on the vocabulary<br>wiki.<br>                                                                                                                   | <br>integer<br>      | <br>Yes<br>          | <br>No<br>              | <br>Yes<br>             | <br>CONCEPT<br>          | <br>Type Concept<br>  |
# MAGIC | **<br>note_class<br>**       | <br>A Standard Concept Id representing the HL7 LOINC Document Type<br>Vocabulary classification of the note.<br>                                                                        | <br>Map the note classification to a Standard Concept. For more information<br>see the ETL Conventions in the description of the NOTE table. Accepted<br>Concepts. This Concept can alternatively be represented by concepts<br>with the relationship ‘Kind of (LOINC)’ to 706391<br>(Note).<br> | <br>integer<br>      | <br>Yes<br>          | <br>No<br>              | <br>Yes<br>             | <br>CONCEPT<br>          | <br>                  |
# MAGIC | **<br>note_title<br>**                  | <br>The title of the note.<br>                                                                                                                                                          | <br>                                                                                                                                                                                                                                                                                             | <br>varchar(250)<br> | <br>No<br>           | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | **<br>note_text<br>**                   | <br>The content of the note.<br>                                                                                                                                                        | <br>                                                                                                                                                                                                                                                                                             | <br>varchar(MAX)<br> | <br>Yes<br>          | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | **<br>encoding<br>**         | <br>This is the Concept representing the character encoding type.<br>                                                                                                                   | <br>Put the Concept Id that represents the encoding character type here.<br>Currently the only option is UTF-8 (32678). It<br>the note is encoded in any other type, like ASCII then put 0.<br>                                                                                                  | <br>integer<br>      | <br>Yes<br>          | <br>No<br>              | <br>Yes<br>             | <br>CONCEPT<br>          | <br>                  |
# MAGIC | **<br>language<br>**         | <br>The language of the note.<br>                                                                                                                                                       | <br>Use Concepts that are descendants of the concept 4182347<br>(World Languages).<br>                                                                                                                                                                                                           | <br>integer<br>      | <br>Yes<br>          | <br>No<br>              | <br>Yes<br>             | <br>CONCEPT<br>          | <br>                  |
# MAGIC | **<br>provider_id<br>**                 | <br>The Provider who wrote the note.<br>                                                                                                                                                | <br>The ETL may need to make a determination on which provider to put here.<br>                                                                                                                                                                                                                  | <br>integer<br>      | <br>No<br>           | <br>No<br>              | <br>Yes<br>             | <br>PROVIDER<br>         | <br>                  |
# MAGIC | **<br>visit_occurrence_id<br>**         | <br>The Visit during which the note was written.<br>                                                                                                                                    | <br>                                                                                                                                                                                                                                                                                             | <br>integer<br>      | <br>No<br>           | <br>No<br>              | <br>Yes<br>             | <br>VISIT_OCCURRENCE<br> | <br>                  |
# MAGIC | <br>visit_detail_id<br>             | <br>The Visit Detail during which the note was written.<br>                                                                                                                             | <br>                                                                                                                                                                                                                                                                                             | <br>integer<br>      | <br>No<br>           | <br>No<br>              | <br>Yes<br>             | <br>VISIT_DETAIL<br>     | <br>                  |
# MAGIC | **<br>note_source_value<br>**           | <br>                                                                                                                                                                                    | <br>The source value mapped to the NOTE_CLASS_CONCEPT_ID.<br>                                                                                                                                                                                                                                    | <br>varchar(50)<br>  | <br>No<br>           | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | <br>note_event_id<br>               | <br>If the Note record is related to another record in the database, this<br>field is the primary key of the linked record.<br>                                                         | <br>Put the primary key of the linked record, if applicable, here.<br>                                                                                                                                                                                                                           | <br>integer<br>      | <br>No<br>           | <br>No<br>              | <br>No<br>              | <br>                     | <br>                  |
# MAGIC | <br>note_event_field<br> | <br>If the Note record is related to another record in the database, this<br>field is the CONCEPT_ID that identifies which table the primary key of<br>the linked record came from.<br> | <br>Put the CONCEPT_ID that identifies which table and field the<br>NOTE_EVENT_ID came from.<br>                                                                                                                                                                                                 | <br>integer<br>      | <br>No<br>           | <br>No<br>              | <br>Yes<br>             | <br>CONCEPT<br>          | <br>                  |
# MAGIC

# COMMAND ----------

import os
os.environ["PYSPARK_PIN_THREAD"]="False"
#import mlflow
#import mlflow.spark

#mlflow.spark.autolog()
#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
#mlflow.end_run()
#run=mlflow.start_run(experiment_id=EXPERIMENT_ID,run_name=run_name)
#run_id=run.info.run_id
#dbutils.jobs.taskValues.set("run_id",run_id)
spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType
import re
import shared.text_process as text_process

# MAGIC %md
# MAGIC     """
# MAGIC         https://github.com/chrismbarr/LyricConverter/blob/865f17613ee8f43fbeedeba900009051c0aa2826/scripts/parser.js#L26-L37
# MAGIC             stripRtf: function(str) {
# MAGIC             var basicRtfPattern = /\{\*?\\[^{}]+;}|[{}]|\\[A-Za-z]+\n?(?:-?\d+)?[ ]?/g;
# MAGIC             var newLineSlashesPattern = /\\\n/g;
# MAGIC             var ctrlCharPattern = /\n\\f[0-9]\s/g;
# MAGIC
# MAGIC             //Remove RTF Formatting, replace RTF new lines with real line breaks, and remove whitespace
# MAGIC             return str
# MAGIC                 .replace(ctrlCharPattern, "")
# MAGIC                 .replace(basicRtfPattern, "")
# MAGIC                 .replace(newLineSlashesPattern, "\n")
# MAGIC             
# MAGIC     """

# COMMAND ----------

import string

# COMMAND ----------

source_table=f"{src_catalog}.{src_schema}.{notetable}"
print(source_table)

# COMMAND ----------

# Extract
source_df = (
    spark
    .table(source_table)
)

display(source_df)

# COMMAND ----------


silver_df = (
    source_df
    .transform(text_process.encoding_decider, "note_text", text_process.proposed)
    .transform(text_process.extract_note_text, "note_text", "encoding")
    .transform(text_process.replace_multi_space, "clean_text")
)
display(silver_df)

# COMMAND ----------

#TODO versioning here? Follow CDH Best Practices
otbl=f"{catalog}.{schema}.{output_table}"
print(otbl)
silver_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl)



