# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)

# COMMAND ----------

install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_lg-0.5.4.tar.gz
%pip freeze

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

# MAGIC %md
# MAGIC This is an auto-generated notebook to perform batch inference on a Spark DataFrame using a selected model from the model registry. This feature is in preview, and we would greatly appreciate any feedback through this form: https://databricks.sjc1.qualtrics.com/jfe/form/SV_1H6Ovx38zgCKAR0.
# MAGIC
# MAGIC ## Instructions:
# MAGIC 1. Run the notebook against a cluster with Databricks ML Runtime version 14.3.x-cpu, to best re-create the training environment.
# MAGIC 2. Add additional data processing on your loaded table to match the model schema if necessary (see the "Define input and output" section below).
# MAGIC 3. "Run All" the notebook.
# MAGIC 4. Note: If the `%pip` does not work for your model (i.e. it does not have a `requirements.txt` file logged), modify to use `%conda` if possible.
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text('MODEL_NAME' , defaultValue= "en_core_sci_lg")
MODEL_NAME = dbutils.widgets.get("MODEL_NAME")

dbutils.widgets.text('LINKER_NAME' , defaultValue= "umls")
LINKER_NAME = dbutils.widgets.get("LINKER_NAME")

dbutils.widgets.text('INPUT_TABLE' , defaultValue="edav_prd_cdh.cdh_abfm_phi_ra.silver_notes")
INPUT_TABLE = dbutils.widgets.get("INPUT_TABLE")

dbutils.widgets.text('OUTPUT_TABLE' , defaultValue= f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_{MODEL_NAME}")
OUTPUT_TABLE = dbutils.widgets.get("OUTPUT_TABLE")

dbutils.widgets.text('TEXT_COL' , defaultValue= "note_text")
TEXT_COL = dbutils.widgets.get("TEXT_COL")

output_col="results_"+LINKER_NAME

# COMMAND ----------

df = spark.table(INPUT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/allenai/scispacy/issues/504#issuecomment-1925367973  
# MAGIC https://stackoverflow.com/questions/63920864/abbreviations-in-natural-language-processing   
# MAGIC   
# MAGIC ```python
# MAGIC  import spacy
# MAGIC     from scispacy.abbreviation import AbbreviationDetector
# MAGIC     nlp=spacy.load("en_core_web_sm")
# MAGIC     abbreviation_pipe=AbbreviationDetector(nlp)
# MAGIC     text="stackoverflow (SO) is a question and answer site for professional and enth_usiast programmers.SO roxks!"
# MAGIC     nlp.add_pipe(abbreviation_pipe)
# MAGIC     def replace_acronyms(text):
# MAGIC        doc=nlp(txt)
# MAGIC        altered_tok=[tok.text for tok in doc]
# MAGIC        print(doc._.abbreviations)
# MAGIC        for abrv in doc._.abbreviations:
# MAGIC           altered_tok[abrv.start]=str(abrv._.long_form)
# MAGIC     return(" "join(altered_tok))
# MAGIC   replace_acronyms(text)
# MAGIC   replace_acronyms("Top executives of Microsoft(MS) and General Motors (GM) met today in NewYord")
# MAGIC   ```

# COMMAND ----------

train="Spinal and bulbar muscular atrophy (SBMA) is an \
           inherited motor neuron disease caused by the expansion \
           of a polyglutamine tract within the androgen receptor (AR). \
           SBMA can be caused by this easily."

# COMMAND ----------


import spacy
from utilities.abbreviation import AbbreviationDetector

nlp = spacy.load(MODEL_NAME)
nlp.max_length = 43793966

abbreviation_pipe = AbbreviationDetector(nlp)
nlp.add_pipe(abbreviation_pipe)

text = [nlp(text, disable = ['ner', 'parser','tagger']) for text in train]  #<---- disable parser, tagger, ner?

text = ' '.join([str(elem) for elem in text]) 

doc = nlp(text)

#Print the Abbreviation and it's definition
print("Abbreviation", "\t", "Definition")
for abrv in doc._.abbreviations:
    print(f"{abrv} \t ({abrv.start}, {abrv.end}) {abrv._.long_form}")

# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice

#.select(['person_id',
#'note_type',
#'provider_id','note_datetime',
#'note_section','note_text'])
#df = spark.table(input_table_name).limit(100)
# optionally, perform additional data processing (may be necessary to conform the schema)
table = df.limit(100).select(['note_text'])
display(table)

#print(hfmodel.predict(data)) #
#output_df = table.withColumn("prediction", loaded_model(struct(*table.columns)))

output_df=table.withColumn(output_col, umls_udf(struct(*table.columns)))

# COMMAND ----------

output_df.display()

# COMMAND ----------

#output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

