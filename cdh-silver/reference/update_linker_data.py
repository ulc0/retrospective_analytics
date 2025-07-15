# Databricks notebook source
# MAGIC %pip freeze
# MAGIC #%pip install -r /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/requirements_scispacy_support.txt
# MAGIC #%pip install scipy==1.10.1
# MAGIC #%restart_python

# COMMAND ----------


from scispacy.linking_utils import KnowledgeBase
from scispacy.candidate_generation import CandidateGenerator, create_tfidf_ann_index
from scispacy.linking import EntityLinker
from scispacy.umls_utils import UmlsKnowledgeBase
from scispacy.abbreviation import AbbreviationDetector

#import scispacy.linking_utils as linking_utils 
#import scispacy.candidate_generation as candidate_generation
#import scispacy.umls_utils as umls_utils
#import scispacy.linking as linking


# COMMAND ----------

import os, tempfile

temp_dir = tempfile.mkdtemp() #.TemporaryDirectory()
print(temp_dir)
os.environ["TEMP_DIR"]=temp_dir
base_dir='/Volumes/edav_prd_cdh/cdh_ml/metadata_data'
kb_path=f"{base_dir}/jsonl/umls_kb.jsonl"
out_dir=base_dir+'/linker/umls'
os.environ["OUT_DIR"]=out_dir


# COMMAND ----------

"""
    tfidf_vectorizer_path = f"{out_dir}/tfidf_vectorizer.joblib"
    ann_index_path = f"{out_dir}/nmslib_index.bin"
    tfidf_vectors_path = f"{out_dir}/tfidf_vectors_sparse.npz"
    concept_aliases_path = f"{out_dir}/concept_aliases.json"
"""

# COMMAND ----------

"""
proformaLinkerPaths = {
    ann_index="https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/data/linkers/2023-04-23/umls/nmslib_index.bin",  # noqa
    tfidf_vectorizer="https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/data/linkers/2023-04-23/umls/tfidf_vectorizer.joblib",  # noqa
    tfidf_vectors="https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/data/linkers/2023-04-23/umls/tfidf_vectors_sparse.npz",  # noqa
    concept_aliases_list="https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/data/linkers/2023-04-23/umls/concept_aliases.json",  # noqa
}
"""

# COMMAND ----------

# MAGIC %md
# MAGIC /Volumes/edav_prd_cdh/cdh_ml/metadata_data/2024AA/META/MRFILES.RRF

# COMMAND ----------

from export_umls_json import main as export_umls_json
export_umls_json(meta_path=f"{base_dir}/2024AA/META",output_path=f"{base_dir}/jsonl/umls_kb.jsonl")

# COMMAND ----------

from create_linker import main as create_linker
create_linker(kb_path,temp_dir)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $TEMP_DIR
# MAGIC cp * $OUT_DIR
# MAGIC ls $OUT_DIR

# COMMAND ----------

# MAGIC %md
# MAGIC import tempfile
# MAGIC from scispacy.candidate_generation import CandidateGenerator, create_tfidf_ann_index, MentionCandidate
# MAGIC import tempfile
# MAGIC  
# MAGIC # use temp_dir, and when done copy:
# MAGIC temp_dir = tempfile.mkdtemp() #.TemporaryDirectory()
# MAGIC os.environ["TEMP_DIR"]=temp_dir
# MAGIC print(temp_dir)
# MAGIC umls_concept_aliases, tfidf_vectorizer, ann_index = create_tfidf_ann_index(temp_dir, kb)
# MAGIC create_tfidf_ann_index(temp_dir, kb)

# COMMAND ----------

# MAGIC %md
# MAGIC ls -ll $OUT_DIR
# MAGIC ls -ll $TEMP_DIR 
# MAGIC cd $TEMP_DIR
# MAGIC cp * $OUT_DIR
# MAGIC ls $OUT_DIR
# MAGIC
