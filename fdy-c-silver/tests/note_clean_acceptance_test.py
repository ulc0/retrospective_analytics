# Databricks notebook source
# base on jsl samples
# https://github.com/JohnSnowLabs/spark-nlp/blob/412ca982aa6cc286411c435dcd5fe1bc005659f5/examples/python/annotation/text/english/document-normalizer/document_normalizer_notebook.ipynb

# COMMAND ----------



import pyspark.sql.functions as F

spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

import os
os.environ['PYSPARK_PIN_THREAD']='False'

import mlflow
import mlflow.data
from mlflow.data.pandas_dataset import PandasDataset
mlflow.autolog()


# COMMAND ----------

dbutils.widgets.text("SENTENCE_TABLE","edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_cdh",)
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="1045730508449824")
dbutils.widgets.text("acceptance_csv","/Volumes/edav_prd_cdh/cdh_ml/metadata_data/acceptance_criteria_abfm_file(in).csv",)

csv=dbutils.widgets.get("acceptance_csv")
stbl=dbutils.widgets.get("SENTENCE_TABLE")
EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------

print(csv)

# COMMAND ----------


accept=spark.read.csv(csv,header=True,multiLine=True).select("note_id","note_sent","expected_clean_text")
display(accept)

# COMMAND ----------

notes=spark.table(stbl)
textcols=notes.schema.names
print(f"Text Columns: {textcols}")

# COMMAND ----------

# compare clean output with expected output
test=(
    notes
    .select('note_id',"note_sent",'note_text')
    .join(accept, (notes.note_id == accept.note_id) & (notes.note_sent == accept.note_sent),"inner")
    .drop(accept.note_sent)
    .drop(accept.note_id)
)

failed=test.filter("NOT note_text==expected_clean_text")

display(failed)

# COMMAND ----------

num_rows=accept.count()
num_fail=failed.count()


# COMMAND ----------

# Construct an MLflow PandasDataset from the Pandas DataFrame, and specify the web URL
# as the source
#acceptance: PandasDataset = mlflow.data.from_spark(accept.toPandas())
#testdata:       PandasDataset = mlflow.data.from_spark(test.toPandas())
#failed:     PandasDataset = mlflow.data.from_spark(failed.toPandas())
acceptance=accept.toPandas().to_dict(orient='records')
failed_set=failed.toPandas().to_dict(orient='records')
with mlflow.start_run(experiment_id=EXPERIMENT_ID):
    # Log the dataset to the MLflow Run. Specify the "training" context to indicate that the
    # dataset is used for model training
    mlflow.log_dict(acceptance, 'acceptance_criteria.json')
    mlflow.log_metric("number failed",num_fail)
    if num_fail>0:
        mlflow.log_dict(failed_set, 'failed_set.json')
    else:
        mlflow.log_param("test",f"****PASSED {num_rows} *****")

