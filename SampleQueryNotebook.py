# Databricks notebook source
event="condition"

# COMMAND ----------

# MAGIC %sql
# MAGIC --- what NOT to DO, conditioncodeconceptmap.Id is the primary join key with condition.CodeConceptMapId
# MAGIC select distinct *
# MAGIC -------- count(*),count(distinct Id)
# MAGIC --  ct.*,
# MAGIC --  m.*
# MAGIC from
# MAGIC   cdh_truveta.conditioncodeconceptmap m
# MAGIC     join 
# MAGIC     cdh_truveta.concept ct
# MAGIC     on m.codeconceptid = ct.conceptid
# MAGIC where ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2','G80.0','G80.1','G80.2','G80.3','G80.4',
# MAGIC             'G80.8','G80.9','F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9',
# MAGIC             'Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79',
# MAGIC             'F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83')
# MAGIC --order by
# MAGIC --  SourceConceptId,
# MAGIC --  CodeConceptId,
# MAGIC --  Id,
# MAGIC --  CodeSystem,
# MAGIC --  ConceptCode
# MAGIC   

# COMMAND ----------

duplicatemap=_sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC --- what NOT to DO, conditioncodeconceptmap.Id is the primary join key with condition.CodeConceptMapId
# MAGIC select
# MAGIC  count(*),count(distinct Id)
# MAGIC --  ct.*,
# MAGIC --  m.*
# MAGIC from
# MAGIC   cdh_truveta.conditioncodeconceptmap m
# MAGIC   where m.codeconceptid in 
# MAGIC   ( select distinct ct.conceptid from
# MAGIC     cdh_truveta.concept ct
# MAGIC ---    on m.codeconceptid = ct.conceptid
# MAGIC where ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2','G80.0','G80.1','G80.2','G80.3','G80.4',
# MAGIC             'G80.8','G80.9','F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9',
# MAGIC             'Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79',
# MAGIC             'F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83')
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --- what NOT to DO, conditioncodeconceptmap.Id is the primary join key with condition.CodeConceptMapId
# MAGIC select
# MAGIC  count(*),count(distinct Id)
# MAGIC --  ct.*,
# MAGIC --  m.*
# MAGIC from
# MAGIC   cdh_truveta.conditioncodeconceptmap m
# MAGIC   where m.codeconceptid in 
# MAGIC   ( select distinct ct.conceptid from
# MAGIC     cdh_truveta.concept ct
# MAGIC ---    on m.codeconceptid = ct.conceptid
# MAGIC where ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2','G80.0','G80.1','G80.2','G80.3','G80.4',
# MAGIC             'G80.8','G80.9','F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9',
# MAGIC             'Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79',
# MAGIC             'F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83')
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- original query
# MAGIC         create or replace table edav_prd_cdh.cdh_ml.truveta_idd_original as
# MAGIC              select distinct c.personid, 
# MAGIC             case  when ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2') then 'IDDA'   
# MAGIC             when ct.conceptcode in ('G80.0','G80.1','G80.2','G80.3','G80.4','G80.8','G80.9') then 'IDDB'  
# MAGIC             when ct.conceptcode in ('F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9') then 'IDDC'  
# MAGIC             when ct.conceptcode in ('Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79') then 'IDDD'  
# MAGIC             when ct.conceptcode in ('F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83') then 'IDDE'  
# MAGIC              end as IDD,
# MAGIC              ct.conceptcode
# MAGIC              from cdh_truveta.condition c 
# MAGIC              --All pts,all diagnosis in universal dx table 
# MAGIC              left join cdh_truveta.conditioncodeconceptmap m 
# MAGIC              on c.codeconceptmapid=m.id 
# MAGIC              left join cdh_truveta.concept ct 
# MAGIC              on m.codeconceptid=ct.conceptid
# MAGIC              ---where IDD is not null  
# MAGIC              order by personid, IDD

# COMMAND ----------

# MAGIC %md
# MAGIC  select p.personid,
# MAGIC             coalesce(IDDA,0) as IDDA,coalesce(IDDB,0) as IDDB,coalesce(IDDC,0) as IDDC
# MAGIC             ,coalesce(IDDD,0) as IDDD,coalesce(IDDE,0) as IDDE, greatest(coalesce(IDDA,0)
# MAGIC             ,coalesce(IDDB,0),coalesce(IDDC,0),coalesce(IDDD,0),coalesce(IDDE,0) ) 
# MAGIC             as IDD from 
# MAGIC             ( select distinct c.personid, 
# MAGIC             case  when ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2') then 'IDDA'   
# MAGIC             when ct.conceptcode in ('G80.0','G80.1','G80.2','G80.3','G80.4','G80.8','G80.9') then 'IDDB'  
# MAGIC             when ct.conceptcode in ('F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9') then 'IDDC'  
# MAGIC             when ct.conceptcode in ('Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79') then 'IDDD'  
# MAGIC             when ct.conceptcode in ('F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83') then 'IDDE'  
# MAGIC              end as IDD 
# MAGIC              from cdh_truveta.condition c 
# MAGIC              --All pts,all diagnosis in universal dx table 
# MAGIC              left join cdh_truveta.conditioncodeconceptmap m 
# MAGIC              on c.codeconceptmapid=m.id 
# MAGIC              left join cdh_truveta.concept ct 
# MAGIC              on m.codeconceptid=ct.conceptid  ) as p  
# MAGIC              pivot (       count(1)      for IDD in ('IDDA','IDDB','IDDC','IDDD','IDDE') ) 

# COMMAND ----------

current_df=_sqldf

# COMMAND ----------

# DBTITLE 1,pyspark with CDH Data Model
#Generate All Patients with Diagnosis IDD=1 and type of diagnosis, and no diagnosis IDD=0 from CONDITION TABLE 
from pyspark.sql import functions as F
import itertools as it
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC This is restricted to condition table for benchmarking. In practice all the cdh_truveta.%codeconceptmap tables would be joined to cdh_truveta.concept, probably after "stacking" (unioning) them
# MAGIC * Id is kept from conditionconceptmap because that is what is on the cdh_truveta.condition table.
# MAGIC * Full spec is to use an custom table with core Athena, augmented by Truveta, Premier etc

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC              from cdh_truveta.condition c 
# MAGIC              --All pts,all diagnosis in universal dx table 
# MAGIC              left join cdh_truveta.conditioncodeconceptmap m 
# MAGIC              on c.codeconceptmapid=m.id 
# MAGIC              left join cdh_truveta.concept ct 
# MAGIC              on m.codeconceptid=ct.conceptid  ) as p  
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Join the Truveta Concept and conditioncodeconceptmap tables ONCE
# MAGIC %md
# MAGIC NO, condition maps to conditionconcept, not the other way around
# MAGIC ```sql
# MAGIC -- adds m.id to concept for join
# MAGIC create or replace table edav_prd_cdh.cdh_ml.conditionconcept_truveta as
# MAGIC select distinct
# MAGIC   ct.*,
# MAGIC   m.Id
# MAGIC from
# MAGIC   cdh_truveta.conditioncodeconceptmap m
# MAGIC     join cdh_truveta.concept ct
# MAGIC       on m.codeconceptid = ct.conceptid
# MAGIC order by
# MAGIC   CodeSystem,
# MAGIC   ConceptCode
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourceconceptid,conceptname --, conceptdefinition, codesystem
# MAGIC from  (select distinct sourceconceptid from 
# MAGIC  cdh_truveta.conditioncodeconceptmap) cm
# MAGIC  join cdh_truveta.concept c
# MAGIC on cm.sourceconceptid=c.conceptid

# COMMAND ----------

sourceconcepts=_sqldf
display(sourceconcepts)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- don't keep keyfields, lets see where the duplicates are
# MAGIC create
# MAGIC or replace table edav_prd_cdh.cdh_ml.truveta_condition as
# MAGIC select
# MAGIC   distinct m.CodeConceptId,
# MAGIC   m.sourceconceptid,
# MAGIC   c.personid,
# MAGIC   c.patientid,
# MAGIC   c.encounterid,
# MAGIC   c.RecordedDateTime,
# MAGIC   c.ClinicalStatusConceptId,
# MAGIC   c.PrimaryDiagnosisConceptId,
# MAGIC   c.BodySiteMapId
# MAGIC from
# MAGIC   cdh_truveta.condition c --All pts,all diagnosis in universal dx table
# MAGIC   left join cdh_truveta.conditioncodeconceptmap m on m.Id = c.CodeConceptMapId
# MAGIC order by
# MAGIC   personid

# COMMAND ----------

condition_concept=spark.table(f"edav_prd_cdh.cdh_ml.truveta_condition")
display(condition_concept.filter(F.isnotnull(F.col("conceptId"))))

# COMMAND ----------

#canonical form? In SQL
raw_IDD = {
    "IDDA": [
        "Q87.11",
        "Q90.0",
        "Q90.1",
        "Q90.2",
        "Q90.9",
        "Q91.0",
        "Q91.1",
        "Q91.2",
        "Q91.3",
        "Q91.4",
        "Q91.5",
        "Q91.6",
        "Q91.7",
        "Q92.0",
        "Q92.1",
        "Q92.2",
        "Q92.5",
        "Q92.62",
        "Q92.7",
        "Q92.8",
        "Q92.9",
        "Q93.0",
        "Q93.1",
        "Q93.2",
        "Q93.3",
        "Q93.4",
        "Q93.5",
        "Q93.51",
        "Q93.59",
        "Q93.7",
        "Q93.81",
        "Q93.88",
        "Q93.89",
        "Q93.9",
        "Q95.2",
        "Q95.3",
        "Q99.2",
    ],
    "IDDB": ["G80.0", "G80.1", "G80.2", "G80.3", "G80.4", "G80.8", "G80.9"],
    "IDDC": ["F84.0", "F84.1", "F84.2", "F84.3", "F84.4", "F84.5", "F84.8", "F84.9"],
    "IDDD": ["Q86.0", "F70", "F71", "F72", "F73", "F78", "F78.A1", "F78.A9", "F79"],
    "IDDE": [
        "F82",
        "F88",
        "F89",
        "G31.81",
        "M95.2",
        "Q04.0",
        "Q04.1",
        "Q04.2",
        "Q04.3",
        "Q04.4",
        "Q04.5",
        "Q04.6",
        "Q04.8",
        "Q04.9",
        "P04.3",
        "Q67.6",
        "Q76.7",
        "Q67.8",
        "Q68.1",
        "Q74.3",
        "Q70.00",
        "Q70.01",
        "Q70.02",
        "Q70.03",
        "Q70.10",
        "Q70.11",
        "Q70.12",
        "Q70.13",
        "Q70.20",
        "Q70.21",
        "Q70.22",
        "Q70.23",
        "Q70.30",
        "Q70.31",
        "Q70.32",
        "Q70.33",
        "Q70.4",
        "Q70.9",
        "Q85.1",
        "Q87.1",
        "Q87.19",
        "Q87.2",
        "Q87.3",
        "Q87.5",
        "Q87.81",
        "Q87.82",
        "Q87.89",
        "Q89.7",
        "Q89.8",
        "R41.83",
    ],
}

discharge_map_dict = {
    "Expired": "09Expired",
    "Custodial/Suportive faciilty": "01Hom Health Org",
    "Alternative home": "01Hom Health Org",
    "Home": "02Home",
    "Hospice": "03Hospice",
    "Left against advice": "10Left Against Advise",
    "Left without being seen": "10Left Against Advise",
    "Long-term care": "07Long Term Care",
    "Other": "04Other Facility",
    "Cancer research/childrens hospital": "04Other Facility",
    "Federal hospital": "04Other Facility",
    "Critical accesss hospital": "04Other Facility",
    "Other healthcare facility": "04Other Facility",
    "Re-admitted": "04Other Facility",
    "Court/law enforcement": "04Other Facility",
    "Psychiatric hospital": "05Psychiatric",
    "Rehabilitation": "08Rehabilitation",
    "Short term general hospital": "08Short Term Care",
    "Still patient": "06SNF",
    "Skilled nursing facility": "06SNF",
}


# COMMAND ----------

# pythonic translation canonical form to a createDataFrame() type 
concepts_dict={}
for concept_set,concept_list in raw_IDD.items():
    for concept_code in concept_list:
        concepts_dict[concept_code]=concept_set
print(concepts_dict)


# COMMAND ----------

# just IDD, but this is the gist of it, all "concept_sets" would be included
features_df=spark.createDataFrame(pd.DataFrame(concepts_dict.items(), columns=["concept_code", "feature"]))
display(features_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #TODO
# MAGIC Truveta ```codeset("CVX", selfAndDescendants, "141")``` 
# MAGIC - define a python function that creates a master dataframe for the map below
# MAGIC ```
# MAGIC def codeset(vocabulary_id,'selfAndDescendants,conceptList):
# MAGIC   
# MAGIC

# COMMAND ----------

# whenever possible use maps for for this effort, only the core file will be partitioned if necessary, lightning fact and Spark efficient
concept_set_map = F.create_map([F.lit(x) for x in it.chain(*concepts_dict.items())])
discharge_map = F.create_map([F.lit(x) for x in it.chain(*discharge_map_dict.items())])

# COMMAND ----------

#now get the IDD codes, and create a feature table (these are CCSRs)
codes_df=spark.table("edav_prd_cdh.cdh_truveta.concept")\
    .withColumn("feature_name",concept_set_map[F.col("ConceptCode")])\
    .filter(F.isnotnull("feature_name"))\
    .orderBy(F.desc("feature_name"))    
# for this exercise keep the non-null, this would be iterative
display(codes_df.filter(F.isnotnull(F.col("feature_name"))))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Now get the concept table matches
# MAGIC #### Two Ways for Benchmarking
# MAGIC - Map of Id
# MAGIC - Join of tables
# MAGIC

# COMMAND ----------

join_condition=condition_concept.join(codes_df,'conceptId','inner')
display(join_condition.filter(F.isnotnull(F.col("conceptId"))))

# COMMAND ----------

# MAGIC %md
# MAGIC  ```sql
# MAGIC  select p.personid,
# MAGIC             coalesce(IDDA,0) as IDDA,coalesce(IDDB,0) as IDDB,coalesce(IDDC,0) as IDDC
# MAGIC             ,coalesce(IDDD,0) as IDDD,coalesce(IDDE,0) as IDDE, greatest(coalesce(IDDA,0)
# MAGIC             ,coalesce(IDDB,0),coalesce(IDDC,0),coalesce(IDDD,0),coalesce(IDDE,0) ) 
# MAGIC             as IDD from 
# MAGIC              pivot (   count(1)      for IDD in ('IDDA','IDDB','IDDC','IDDD','IDDE') ) 
# MAGIC ```

# COMMAND ----------

final_df=condition_concept.withColumn("IDD",F.lit(1))\
  .groupby(F.col("patientId"))\
  .pivot("feature_name")\
  .sum("IDD")
  

# COMMAND ----------

display(final_df)