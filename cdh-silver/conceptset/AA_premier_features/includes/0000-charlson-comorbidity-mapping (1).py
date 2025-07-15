# Databricks notebook source
import requests
import pandas as pd
import itertools

import pyspark.sql.functions as F

# COMMAND ----------

comorbidipy_mapping_raw = requests.get("https://raw.githubusercontent.com/vvcb/comorbidipy/main/comorbidipy/mapping.py")

if comorbidipy_mapping_raw.status_code == 200:
    exec(comorbidipy_mapping_raw.content)
else:
    print("Failed to pull file...")
    
del comorbidipy_mapping_raw

mapping_options = list(mapping.keys())

print("Mapping Options: ", mapping_options, end="\n\n")
print("Sample Mapping: ", mapping["charlson_icd10_shmi"], end="\n\n")

dbutils.widgets.dropdown("mapping_option", "charlson_icd10_shmi", mapping_options)

# COMMAND ----------

MAPPING_OPTION_SELECTED = dbutils.widgets.get("mapping_option")

print(f"You have selected (mapping={MAPPING_OPTION_SELECTED})...", "Generating mapping expression...")

try:
    selected_mapping = mapping[MAPPING_OPTION_SELECTED]
    selected_mapping__reverse = { v: k for k, l in selected_mapping.items() for v in l }

    mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*selected_mapping__reverse.items())])

    print(f"""
    Mapping Generation Complete. You can access the Spark mapping via `mapping_expr`.
    
    This mapping converts ICD10 codes to a corresponding comorbidity.
    
    Example usage:
    ```
    df_output = (
        df_input
            .select(
                mapping_expr[F.col("icd_code")].alias("condition")
            )
    )
    ```
    
    Other Variables:
    - `mapping`: fetched python dictionary of mappings
    - `mapping_options`: mapping options (keys of `mapping` dictionary)
    - `selected_mapping`: mapping for selected map option
    - `selected_mapping__reverse`: reversed mapping
    """)
    
except Exception as e:
    print("Failed to generate mapping")
    raise e

# COMMAND ----------


