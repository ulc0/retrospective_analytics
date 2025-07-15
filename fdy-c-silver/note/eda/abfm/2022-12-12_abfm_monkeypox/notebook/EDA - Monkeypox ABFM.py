# Databricks notebook source
spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp
import pyspark.sql.functions as F
spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

pip install transformers

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("publichealthsurveillance/PHS-BERT")
model = AutoModel.from_pretrained("publichealthsurveillance/PHS-BERT")

# COMMAND ----------

# MAGIC %md ## Tables to explore in ABFM
# MAGIC
# MAGIC 1. visitdiagnosis
# MAGIC 2. patientsocialhistoryobservation
# MAGIC 3. patientnoteresultobservation
# MAGIC 4. patientnoteprocedure
# MAGIC 5. patientnoteproblem
# MAGIC 6. patientnote
# MAGIC 7. patientproblem

# COMMAND ----------

# MAGIC %run /CDH/Analytics/ABFM_PHI/Projects/cdh-abfm-phi-core/YYYY-MM-DD_abfm_template_notebooks/includes/0000-utils-high

# COMMAND ----------

visitDiag =spark.table("edav_prd_cdh.cdh_abfm_phi.visitdiagnosis")
visit_diagnosis_cleaned = load_visitdiagnosis_table()
visit  = spark.table("edav_prd_cdh.cdh_abfm_phi.visit")
pxSocHis = spark.table("edav_prd_cdh.cdh_abfm_phi.patientsocialhistoryobservation")
pxResultObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientresultobservation")
pxProcedure =spark.table("edav_prd_cdh.cdh_abfm_phi.patientprocedure")
pxProm = spark.table("edav_prd_cdh.cdh_abfm_phi.patientproblem")
pxNoteResObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation") 
pxNoteProc = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteprocedure") 
pxNoteProb = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteproblem")
#pxNote = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnote")
pxImunizaion = spark.table("edav_prd_cdh.cdh_abfm_phi.patientimmunization")



# COMMAND ----------

# MAGIC %md # Visit diagnosis

# COMMAND ----------

display(visit_diagnosis_cleaned)

# COMMAND ----------

display(visit_diagnosis_cleaned    
    .where("""
              lower(encounterdiagnosistext) like "%monkypox%"
           or lower(encounterdiagnosistext) like "%monkey pox%"
           or lower(encounterdiagnosistext) like "%monkepox pox%"
           or lower(encounterdiagnosistext) like "%monky pox%"
           or lower(encounterdiagnosistext) like "%monkey-pox%"
           or lower(encounterdiagnosistext) like "%monke-pox%"
           or lower(encounterdiagnosistext) like "%monky-pox%"
           or lower(encounterdiagnosistext) like "%orthopox%"
           or lower(encounterdiagnosistext) like "%monk%"           
           or lower(encounterdiagnosistext) like "%ortho pox%"
           or lower(encounterdiagnosistext) like "%poxvi%"      
           or lower(encounterdiagnosistext) like "%monkeypox%" 
           and encounterdate > 2022-01-01""")        
        .groupBy("encounterdiagnosiscode")
        .agg(
            F.count("encounterdiagnosiscode").alias("count"),
            F.collect_set("encounterdiagnosistext").alias("string monkeypox")            
        )                
        .orderBy(F.desc("count"))
        
)



# COMMAND ----------

display(visitDiag    
    .where("""encounterdiagnosiscode like "%B04%" 
           or encounterdiagnosiscode like "359811007"
           or encounterdiagnosiscode like "359814004"
           or encounterdiagnosiscode like "59774002"
           or encounterdiagnosiscode like "1003839"
           or encounterdiagnosiscode like "1004340"
           or encounterdiagnosiscode like "05900"
           or encounterdiagnosiscode like "059%"
           or encounterdiagnosiscode like "O5900"            
           
           or encounterdiagnosistext like "%monk%"           
           or encounterdiagnosistext like "%ortho pox"
           or encounterdiagnosistext like "%poxvi%"      
           or encounterdiagnosistext like "%monkeypox%" 
           """).sort("encounterdate", ascending = False)
)

display(
    visit_diagnosis_cleaned
    .where("""encounterdiagnosiscode like "B04" 
           or encounterdiagnosiscode like "359811007"
           or encounterdiagnosiscode like "359814004"
           or encounterdiagnosiscode like "59774002"
           or encounterdiagnosiscode like "1003839"
           or encounterdiagnosiscode like "1004340"
           or encounterdiagnosiscode like "59774002"
           or encounterdiagnosiscode like "05900"
           or encounterdiagnosiscode like "O5900"  
           or encounterdiagnosiscode like "O5909"  
           or encounterdiagnosiscode like "O5901"  
          
           or encounterdiagnosistext like "%monk%"           
           or encounterdiagnosistext like "%ortho pox"
           or encounterdiagnosistext like "%poxvi%" 
           or encounterdiagnosistext like "%monkeypox%" 
           """).sort("encounterdate", ascending = False)
)


# COMMAND ----------

# MAGIC %md # Visit

# COMMAND ----------

display(
    visit.where("""encountertypecode like "B04" 
           or encountertypecode like "359811007"
           or encountertypecode like "359814004"
           or encountertypecode like "59774002"
           or encountertypecode like "05900"
           or encountertypecode like "O5900"
           or encountertypecode like "059.00"
           or encountertypecode like "1003839"
           or encountertypecode like "1004340"
           
           
           or lower(encountertypetext) like "%monkypox%"
           or lower(encountertypetext) like "%monkey pox%"
           or lower(encountertypetext) like "%monkepox pox%"
           or lower(encountertypetext) like "%monky pox%"
           or lower(encountertypetext) like "%monkey-pox%"
           or lower(encountertypetext) like "%monke-pox%"
           or lower(encountertypetext) like "%monky-pox%"
           or lower(encountertypetext) like "%orthopox%"
           or lower(encountertypetext) like "%monk%"           
           or lower(encountertypetext) like "%ortho pox%"
           or lower(encountertypetext) like "%poxvi%"      
           or lower(encountertypetext) like "%monkeypox%"  
           
           or lower(note) like "%monkypox%"
           or lower(note) like "%monkey pox%"
           or lower(note) like "%monkepox pox%"
           or lower(note) like "%monky pox%"
           or lower(note) like "%monkey-pox%"
           or lower(note) like "%monke-pox%"
           or lower(note) like "%monky-pox%"
           or lower(note) like "%orthopox%"
           or lower(note) like "%monk%"           
           or lower(note) like "%ortho pox%"
           or lower(note) like "%poxvi%"      
           or lower(note) like "%monkeypox%"  
           """).sort("encounterstartdate", ascending = False)
)
 

# COMMAND ----------

# MAGIC %md #patientsocialhistoryobservation

# COMMAND ----------

display(
    pxSocHis
    .where("""socialhistorytypetext like "%monke%"   
           or socialhistorytypetext like "%ortho pox"
           or socialhistorytypetext like "%poxvi%"  
           or socialhistorystatustext like "%monk%"   
           or socialhistorystatustext like "%ortho pox"
           or socialhistorystatustext like "%poxvi%"  
           or socialhistoryobservedvalue like "%monke%"   
           or socialhistoryobservedvalue like "%ortho pox"
           or socialhistoryobservedvalue like "%poxvi%" 
           
           
           """).sort("documentationdate", ascending = False)
)

# COMMAND ----------

# MAGIC %md #patientprocedureobservation

# COMMAND ----------

display(
    pxResultObs
    .where("""practicecode like "B04" 
           or practicecode like "359811007"
           or practicecode like "359814004"
           or practicecode like "59774002"
           or practicecode like "05900"
           or practicecode like "O5900"            
          
          or practicedescription like "%monk%"           
           or practicedescription like "%ortho pox"
           or practicedescription like "%poxvi%"       
           
           or resultorderdescription like "%monk%"
           or resultorderdescription like "%ortho pox"
           or resultorderdescription like "%poxvi%"  
           
           or proceduretext like "%monk%"
           or proceduretext like "%ortho pox"
           or proceduretext like "%poxvi%"
           
           or methodcodetext like "%monk%"
           or methodcodetext like "%ortho pox"
           or methodcodetext like "%poxvi%"           
           or methodcodetext like "B04"
           or methodcodetext like "359811007"
           or methodcodetext like "359814004"           
           or methodcodetext like "59774002"
           or methodcodetext like "05900"
           or methodcodetext like "O5900"
           
           or observationdescription like "%monk%"
           or observationdescription like "%ortho pox"
           or observationdescription like "%poxvi%"

           """).sort("resultdate", ascending = False)
)

# COMMAND ----------

# MAGIC %md #patientprocedure

# COMMAND ----------

display(
    pxProcedure
    .where("""procedurecode like "B04" 
           or procedurecode like "359811007"
           or procedurecode like "359814004"
           or procedurecode like "59774002"
           or procedurecode like "05900"
           or procedurecode like "O5900" 
           or procedurecode like "87593"
           or procedurecode like "90611"
           
           or proceduredescription like "%monk%"           
           or proceduredescription like "%ortho pox"
           or proceduredescription like "%poxvi%"   
           or proceduredescription like "%monkeypox%" 
           
           or procedurenote like "%monk%"           
           or procedurenote like "%ortho pox"
           or procedurenote like "%poxvi%"
           or procedurenote like "%monkeypox%"
           """).sort("effectivedate", ascending = False)
)

# COMMAND ----------

# MAGIC %md # patientproblem

# COMMAND ----------

display(
    pxProm
    .where("""problemcode like "B04" 
           or problemcode like "359811007"
           or problemcode like "359814004"
           or problemcode like "59774002"
           or problemcode like "05900"
           or problemcode like "O5900"
           or problemcode like "1003839"
           or problemcode like "1004340"
           
           or problemstatuscode like "B04"
           or problemstatuscode like "359811007"
           or problemstatuscode like "359814004"
           or problemstatuscode like "59774002"
           or problemstatuscode like "05900"
           or problemstatuscode like "O5900"
           or problemstatuscode like "1003839"
           or problemstatuscode like "1004340"
           
           
           or problemtext like "%monk%"           
           or problemtext like "%ortho pox"
           or problemtext like "%poxvi%" 
           or problemtext like "%monkeypox%" 
           or problemcomment like "%monk%"
           or problemcomment like "%ortho pox"
           or problemcomment like "%poxvi%" 
           or problemcomment like "%monkeypox%"
           """).sort("documentationdate", ascending = False)
)
    

# COMMAND ----------

# MAGIC %md # patientnoteresultsobservation

# COMMAND ----------

display(
    pxNoteResObs
    .where("""lower(note) like "%monkypox%"
           or lower(note) like "%monkey pox%"
           or lower(note) like "%monkepox pox%"
           or lower(note) like "%monky pox%"
           or lower(note) like "%monkey-pox%"
           or lower(note) like "%monke-pox%"
           or lower(note) like "%monky-pox%"
           or lower(note) like "%orthopox%"
           or lower(note) like "%monk%"           
           or lower(note) like "%ortho pox%"
           or lower(note) like "%poxvi%"      
           or lower(note) like "%monkeypox%"
           and lower(note) not like "%uber%"
           """
           ).sort("encounterdate", ascending = False)
)

# COMMAND ----------

# MAGIC %md # patientnoteprocedure

# COMMAND ----------

display(
    pxNoteProc
    .where("""lower(note) like "%monkypox%"
           or lower(note) like "%monkey pox%"
           or lower(note) like "%monkepox pox%"
           or lower(note) like "%monky pox%"
           or lower(note) like "%monkey-pox%"
           or lower(note) like "%monke-pox%"
           or lower(note) like "%monky-pox%"
           or lower(note) like "%orthopox%"
           or lower(note) like "%monk%"           
           or lower(note) like "%ortho pox%"
           or lower(note) like "%poxvi%"      
           or lower(note) like "%monkeypox%"           
           """
           ).sort("encounterdate", ascending = False)
)

pxNoteProc.printSchema()


# COMMAND ----------

# MAGIC %md # Patient note problem

# COMMAND ----------

display(
    pxNoteProb
    .where("""
              note like "%B04%"
           or note like "%359811007%"
           or note like "%359814004%"
           or note like "%59774002%"
           or note like "%05900%"
           or note like "%O5900%"
           or note like "%monk%"           
           or note like "%ortho pox"
           or note like "%poxvi%" 
           
           or group1 like "%B04%"
           or group1 like "%359811007%"
           or group1 like "%359814004%"
           or group1 like "%59774002%"
           or group1 like "%05900%"
           or group1 like "%O5900%"
           or group1 like "%monk%"           
           or group1 like "%ortho pox"
           or group1 like "%poxvi%"
           
           or group2 like "%B04%"
           or group2 like "%359811007%"
           or group2 like "%359814004%"
           or group2 like "%59774002%"
           or group2 like "%05900%"
           or group2 like "%O5900%"
           or group2 like "%monk%"           
           or group2 like "%ortho pox"
           or group2 like "%poxvi%"
           
           or group3 like "%B04%"
           or group3 like "%359811007%"
           or group3 like "%359814004%"
           or group3 like "%59774002%"
           or group3 like "%05900%"
           or group3 like "%O5900%"
           or group3 like "%monk%"           
           or group3 like "%ortho pox"
           or group3 like "%poxvi%"
           
           or group4 like "%B04%"
           or group4 like "%359811007%"
           or group4 like "%359814004%"
           or group4 like "%59774002%"
           or group4 like "%05900%"
           or group4 like "%O5900%"
           or group4 like "%monk%"           
           or group4 like "%ortho pox"
           or group4 like "%poxvi%"
                      
     """).sort("encounterdate", ascending = False)          
)

# COMMAND ----------

# MAGIC %md # patientnote

# COMMAND ----------

display(pxNote
        .where("""          
              sectionname like "%monk%"           
           or sectionname like "%ortho pox"
           or sectionname like "%poxvi%"   
           or note like "%monk%"           
           or note like "%ortho pox"
           or note like "%poxvi%" 
           """).sort("encounterdate", ascending = False)
       )

# COMMAND ----------

# MAGIC %md # Patient lab order

# COMMAND ----------

labOrder = spark.table("edav_prd_cdh.cdh_abfm_phi.patientlaborder")


# COMMAND ----------

display(
    labOrder
    .where("""practicecode like "B04" 
           or practicecode like "359811007"
           or practicecode like "359814004"
           or practicecode like "59774002"
           or practicecode like "1003839"
           or practicecode like "1004340"
           or practicecode like "59774002"
           or practicecode like "05900"
           or practicecode like "O5900"  
           or practicecode like "O5909"  
           or practicecode like "O5901"  
          
           or description like "%monk%"           
           or description like "%ortho pox"
           or description like "%poxvi%" 
           or description like "%monkeypox%" 
           """).sort("orderdate", ascending = False)
)

# COMMAND ----------

display(labOrder    
    .where("""
              lower(description) like "%monkypox%"
           or lower(description) like "%monkey pox%"
           or lower(description) like "%monkepox pox%"
           or lower(description) like "%monky pox%"
           or lower(description) like "%monkey-pox%"
           or lower(description) like "%monke-pox%"
           or lower(description) like "%monky-pox%"
           or lower(description) like "%orthopox%"
           or lower(description) like "%monk%"           
           or lower(description) like "%ortho pox%"
           or lower(description) like "%poxvi%"      
           or lower(description) like "%monkeypox%" 
           and orderdate > 2020-05-22""")        
        .groupBy("practiceid")
        .agg(
            F.count("practiceid").alias("count"),
            F.collect_set("description").alias("string monkeypox")            
        )                
        .orderBy(F.desc("count"))
        
)


# COMMAND ----------

display(labOrder)

# COMMAND ----------

# MAGIC %md ## Paient imunization 

# COMMAND ----------

display(
    pxImunizaion
    .where("""procedurecode like "B04" 
           or procedurecode like "359811007"
           or procedurecode like "359814004"
           or procedurecode like "59774002"
           or procedurecode like "1003839"
           or procedurecode like "1004340"
           or procedurecode like "59774002"
           or procedurecode like "05900"
           or procedurecode like "O5900"  
           or procedurecode like "O5909"  
           or procedurecode like "O5901"  
          
           or immunizationname like "%monk%"           
           or immunizationname like "%ortho pox"
           or immunizationname like "%poxvi%" 
           or immunizationname like "%monkeypox%" 
           """).sort("immunizationstartdate", ascending = False)
)



# COMMAND ----------

# MAGIC %md #patientnoteresultobservation 

# COMMAND ----------

pxResObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation")

# COMMAND ----------

display(
    pxResObs
    .where("""
              note like "%B04%"
           or note like "%359811007%"
           or note like "%359814004%"
           or note like "%59774002%"
           or note like "%05900%"
           or note like "%O5900%"
           or note like "%monk%"           
           or note like "%ortho pox"
           or note like "%poxvi%" 
           or note like "O5909"  
           or note like "O5901"
           
           or group1 like "%B04%"
           or group1 like "%359811007%"
           or group1 like "%359814004%"
           or group1 like "%59774002%"
           or group1 like "%05900%"
           or group1 like "%O5900%"
           or group1 like "%monk%"           
           or group1 like "%ortho pox"
           or group1 like "%poxvi%"
           or group1 like "O5909"  
           or group1 like "O5901"
           
           or group2 like "%B04%"
           or group2 like "%359811007%"
           or group2 like "%359814004%"
           or group2 like "%59774002%"
           or group2 like "%05900%"
           or group2 like "%O5900%"
           or group2 like "%monk%"           
           or group2 like "%ortho pox"
           or group2 like "%poxvi%"
           or group2 like "O5909"  
           or group2 like "O5901"
           
           or group3 like "%B04%"
           or group3 like "%359811007%"
           or group3 like "%359814004%"
           or group3 like "%59774002%"
           or group3 like "%05900%"
           or group3 like "%O5900%"
           or group3 like "%monk%"           
           or group3 like "%ortho pox"
           or group3 like "%poxvi%"
           or group3 like "O5909"  
           or group3 like "O5901"
           
           or group4 like "%B04%"
           or group4 like "%359811007%"
           or group4 like "%359814004%"
           or group4 like "%59774002%"
           or group4 like "%05900%"
           or group4 like "%O5900%"
           or group4 like "%monk%"           
           or group4 like "%ortho pox"
           or group4 like "%poxvi%"
           or group4 like "O5909"  
           or group4 like "O5901"
                      
     """).sort("encounterdate", ascending = False)   
)

# COMMAND ----------

display(pxResObs)
