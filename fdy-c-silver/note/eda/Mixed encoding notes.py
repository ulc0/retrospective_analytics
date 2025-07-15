# Databricks notebook source
# MAGIC %sql
# MAGIC select * from edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes_rtf
# MAGIC where provider_id=946
# MAGIC union
# MAGIC select * from edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes_utf
# MAGIC where provider_id=946
# MAGIC order by person_id, note_datetime
# MAGIC
