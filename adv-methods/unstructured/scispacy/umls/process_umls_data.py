# Databricks notebook source
# MAGIC %md
# MAGIC def main(
# MAGIC         meta_path: str,
# MAGIC         output_path: str,
# MAGIC         lang: str = None,
# MAGIC         source: str = None,
# MAGIC         non_suppressed: bool = True,
# MAGIC     ):
# COMMAND ----------

# MAGIC %sh
# MAGIC python export_umls_json.py --meta_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/umls/2024AA/META/ --output_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/umls_mrconso_2024AA.jsonl
# MAGIC
from scispacy.scripts import export_umls_json


# COMMAND ----------

# MAGIC %sh
# MAGIC #python3 export_umls_json.py --meta_path ~/Documents/Taxonomy/UMLS/2022AB/META --output_path ../output/umls_kb.jsonl
# MAGIC python3 create_linker.py --kb_path ./Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/umls_mrconso_2024AA.jsonl --output_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/umls_kb/
