# Databricks notebook source
# MAGIC %md
# MAGIC # Outline
# MAGIC 1. Get Target Visits
# MAGIC   - done and saved
# MAGIC   - needs one additional change to update (drop a few unwanted cols)
# MAGIC 2. Get Outcomes on Target Visits
# MAGIC   - need to prefilter on target visits and then run through again
# MAGIC   - write just the outcomes on the target vists
# MAGIC       1. MISA
# MAGIC       2. Death
# MAGIC       - need to generate
# MAGIC       3. ICU
# MAGIC       - need to generate
# MAGIC 3. Get Features
# MAGIC   - prefilter on patients with target encounter
# MAGIC   - then select encounters based on this criteria
# MAGIC   - finish link to the premier data
