# Databricks notebook source
dbutils.widgets.text("DBCATALOG","edav_prd_cdh")
dbutils.widgets.text("DEST_SCHEMA","cdh_premier_ra")
DBCATALOG=dbutils.widgets.get("DBCATALOG")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")
tables=['hcpcs','loinc','icd','snomed','patbill_stdchg','patbill_hospchg']
keys=['person_id','visit_start_date','visit_occurrence_number']

# COMMAND ----------

for table in tables:
    for key in keys:
        sqlcode=f"ALTER TABLE {DBCATALOG}.{DEST_SCHEMA}.fact_{table}_events ALTER COLUMN {key} SET NOT NULL;"
        print(sqlcode)
        results=spark.sql(sqlcode)
        print(results)
    sqlcode=f"ALTER TABLE {DBCATALOG}.{DEST_SCHEMA}.fact_{table}_events "
    sqlcode=sqlcode+f" ADD CONSTRAINT fact_premier_{table}_person_date_number "
    sqlcode=sqlcode+" PRIMARY KEY(person_id, visit_start_date TIMESERIES,visit_occurrence_number) "
    print(sqlcode)
    results=spark.sql(sqlcode)
    print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ALTER TABLE ${table_name} ADD CONSTRAINT fact_premier_encounter_person_date PRIMARY KEY(person_id,visit_occurrence_number, visit_start_datetime TIMESERIES)
# MAGIC ALTER TABLE ${table_name} ADD CONSTRAINT premier_cpt_person_date PRIMARY KEY(person_id, visit_start_date TIMESERIES, visit_occurrence_number)
# MAGIC ALTER TABLE ${table_name} ADD CONSTRAINT fact_premier_encounter_person_date PRIMARY KEY(person_id,visit_occurrence_number, visit_start_datetime TIMESERIES)
# MAGIC ALTER TABLE  ${table_name} ADD CONSTRAINT premier_icd_person_date PRIMARY KEY(person_id, visit_start_date TIMESERIES,visit_occurrence_number)
# MAGIC ALTER TABLE ${table_name} ADD CONSTRAINT premier_loinc_person_date PRIMARY KEY(person_id, visit_start_date TIMESERIES,visit_occurrence_number);
# MAGIC ALTER TABLE fact_premier_${concept.name}_events ADD CONSTRAINT premier_${concept.name}_person_date PRIMARY KEY(person_id, visit_start_date TIMESERIES,visit_occurrence_number)
# MAGIC ALTER TABLE ${table_name} ADD CONSTRAINT premier_snomed_person_date PRIMARY KEY(person_id, visit_start_date TIMESERIES,visit_occurrence_number)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE edav_prd_cdh.cdh_premier_ra.fact_hcpcs_events  ADD CONSTRAINT fact_premier_hcpcs_person_date_number ;
