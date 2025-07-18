# Databricks notebook source
# MAGIC %pip freeze
# MAGIC %pip install scipy==1.10.1
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC umls download path  
# MAGIC /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/2024AA/META/ 
# MAGIC json path   
# MAGIC /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/  
# MAGIC * 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ll /dbfs/kb
# MAGIC #mkdir /dbfs/kb/jsonl
# MAGIC ls -ll /dbfs/kb/jsonl

# COMMAND ----------

# MAGIC %sh
# MAGIC #python3 export_umls_json.py --meta_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/2024AA/META --output_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/umls_kb.jsonl
# MAGIC python3 export_umls_json.py --meta_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/2024AA/META --output_path /dbfs/kb/jsonl/umls_kb.jsonl
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC python3 create_linker.py --kb_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/umls_kb.jsonl --output_path /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl

# COMMAND ----------


from scispacy.linking_utils import KnowledgeBase
kb_path='/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl/umls_kb.jsonl'
kb_path='/dbfs/kb/jsonl/umls_kb.jsonl'

kb = KnowledgeBase(file_path=kb_path)

# COMMAND ----------

import tempfile
from scispacy.candidate_generation import CandidateGenerator, create_tfidf_ann_index, MentionCandidate

with tempfile.TemporaryDirectory() as dir_name:
    umls_concept_aliases, tfidf_vectorizer, ann_index = create_tfidf_ann_index(dir_name, kb)


# COMMAND ----------

UmlsLinkerPaths = LinkerPaths(
    ann_index="nmslib_index.bin",  # noqa
    tfidf_vectorizer="tfidf_vectorizer.joblib",  # noqa
    tfidf_vectors="tfidf_vectors_sparse.npz",  # noqa
    concept_aliases_list="concept_aliases.json",  # noqa
)


# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp

# COMMAND ----------



# COMMAND ----------

output_path='dbfs:/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl'
#output_path='abfss://cdh@davsynapseanalyticsdev.dfs.core.windows.net/machinelearning/scispacy/kbs'
#output_path='/dbfs/kb'
#output_path='/Workspace/CDH'
#output_path='/Workspace/Shared/scispacy'
#os.makedirs(output_path, exist_ok=True)
output_path='.'


# COMMAND ----------

import os
from scispacy.candidate_generation import create_tfidf_ann_index

output_path='dbfs:/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/jsonl'
#output_path='abfss://cdh@davsynapseanalyticsdev.dfs.core.windows.net/machinelearning/scispacy/kbs'
#output_path='/dbfs/kb'
#output_path='/Workspace/CDH'
#output_path='/Workspace/Shared/scispacy'
#os.makedirs(output_path, exist_ok=True)
output_path='.'

#create_tfidf_ann_index(output_path, kb)
