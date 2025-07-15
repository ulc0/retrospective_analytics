# Databricks notebook source
import os
os.environ['HF_HUB_DISABLE_PROGRESS_BARS']=1
os.environ['TRANSFORMERS_CACHE']='/dbfs/mnt/ml/transformers/cache/'
os.environ['SCISPACY_CACHE']='/dbfs/mnt/ml/scispacy_cache/'
os.environ['MLFLOW_TRACKING_URI']='databricks'
os.environ['HUGGINGFACE_HUB_CACHE']='/dbfs/mnt/ml/huggingface/cache/'
