# Databricks notebook source
cohortList=[9311489129585,
7859790187666,
197568527897,
116531052851634,
17197049086455,
15522011834113,
67997922668980,
112382114303306,
369367221096,
12017318530663,
34471407550352,
26157,
30247,
34273839050941,
2954937528788,
30236569792991,
35986,
17179900724,
60129576389,
31301721686465,
3865470600220,
20263655738961,
3633542364311,
114254720014135,
32624571612859,
54107998378894,
82609401181045,
89678917543012,
108800111757331,
214748399689,
17179910766,]

from pyspark.sql.types import IntegerType, Row

l = map(lambda x : Row(x), cohortList)
# notice the parens after the type name
cohort=spark.createDataFrame(l,["note_id"])

display(cohort)

# COMMAND ----------

bronze_note=spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_bronze")
joined=bronze_note.join(cohort, bronze_note.note_id==cohort.note_id, "inner")
display(joined)
