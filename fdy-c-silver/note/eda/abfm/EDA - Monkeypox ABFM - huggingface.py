# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC install "tensorflow>=2.0.0"

# COMMAND ----------

# MAGIC %md
# MAGIC install --upgrade tensorflow-hub

# COMMAND ----------

# MAGIC %md
# MAGIC install transformers

# COMMAND ----------

# MAGIC %md
# MAGIC install torch 

# COMMAND ----------

# MAGIC %md
# MAGIC install torchvision

# COMMAND ----------

# MAGIC %md
# MAGIC install transformers datasets

# COMMAND ----------

spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

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

tblList=["visitdiagnosis",
"patientsocialhistoryobservation",
"patientnoteresultobservation",
"patientnoteprocedure",
"patientnoteproblem",
"patientnote",
"patientproblem"]

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
pxNote = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnote")
pxImunizaion = spark.table("edav_prd_cdh.cdh_abfm_phi.patientimmunization")



# COMMAND ----------

# MAGIC %md ## Cleaning functions

# COMMAND ----------

def cleanDate(date):
    return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")

# COMMAND ----------

def deleteMarkup(colNote):        
     rft = regexp_replace(colNote, r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","") 
     xlm = regexp_replace(rft, r"<[^>]*>|&[^;]+;", "")
     font_header1 = regexp_replace(xlm, "Tahoma;;", "")
     font_header2 = regexp_replace(font_header1, "Arial;Symbol;| Normal;heading 1;", "")
     font_header3 = regexp_replace(font_header2, "Segoe UI;;", "")
     font_header4 = regexp_replace(font_header3, "MS Sans Serif;;", "")
     return font_header4       


# COMMAND ----------

words_to_match_mpox = (
    [
    'monkeypox', 'monkypox', 'monkey pox', 
    'monkepox pox', 'monky pox', 'monkey-pox', 
    'monke-pox', 'monky-pox', 'orthopox',
    'orthopoxvirus', 'parapoxvirus',
    'ortho pox', 'poxvi', 'monkeypox', 
    'mpox', 'm-pox', 'mpx'
    ]
)

# COMMAND ----------


def find_partial_matches(df, words_to_match, columns_to_search):
    concat_col = (
        df.select("patientuid", "clean_date", *columns_to_search,
                  concat(*[ concat(F.lit(" "),(F.lower(F.col(column)))) for column in columns_to_search],F.lit(" ")).alias("concat_col_temp")
                  )
        )
    regex_pattern = "|".join(words_to_match)
    filtered_sdf = concat_col.filter(F.col("concat_col_temp").rlike(regex_pattern))
    return filtered_sdf


# COMMAND ----------

time_variable = "encounterdate"
note_variables = ['sectionname', 'cleaned_note']

treated_note = pxNote.select("*", cleanDate(time_variable).alias("clean_date"), deleteMarkup("note").alias("cleaned_note"))
mpox_notes = find_partial_matches(treated_note, words_to_match_mpox, note_variables)

display(mpox_notes)

# COMMAND ----------

# MAGIC %md # Visit diagnosis

# COMMAND ----------

display(visit_diagnosis_cleaned)

# COMMAND ----------

words_to_match_mpox = (
    [
    'monkeypox', 'monkypox', 'monkey pox', 
    'monkepox pox', 'monky pox', 'monkey-pox', 
    'monke-pox', 'monky-pox', 'orthopox',
    'orthopoxvirus', 'parapoxvirus',
    'ortho pox', 'poxvi', 'monkeypox', 
    'mpox', 'm-pox', 'mpx'
    ]
)

# COMMAND ----------

display(visit_diagnosis_cleaned    
    .where("""
              encounterdiagnosiscode like "B04" 
           or encounterdiagnosiscode like "359811007"
           or encounterdiagnosiscode like "359814004"
           or encounterdiagnosiscode like "59774002"
           or encounterdiagnosiscode like "1003839"
           or encounterdiagnosiscode like "1004340"
           or encounterdiagnosiscode like "05900"
           or encounterdiagnosiscode like "059%"                              
           or encounterdiagnosiscode like "414015000"  
           or encounterdiagnosiscode like "B0869"
           or encounterdiagnosiscode like "B0860"
           or encounterdiagnosiscode like "59010"


           and lower(encounterdiagnosistext) like "%monkeypox%"
           and lower(encounterdiagnosistext) like "%monkypox%"
           and lower(encounterdiagnosistext) like "%monkey pox%"
           and lower(encounterdiagnosistext) like "%monkepox pox%"
           and lower(encounterdiagnosistext) like "%monky pox%"
           and lower(encounterdiagnosistext) like "%monkey-pox%"
           and lower(encounterdiagnosistext) like "%monke-pox%"
           and lower(encounterdiagnosistext) like "%monky-pox%"
           and lower(encounterdiagnosistext) like "%orthopox%"
           and lower(encounterdiagnosistext) like "%orthopoxvirus%"
           and lower(encounterdiagnosistext) like "%parapoxvirus%"           
           and lower(encounterdiagnosistext) like "%monk%"           
           and lower(encounterdiagnosistext) like "%ortho pox%"
           and lower(encounterdiagnosistext) like "%poxvi%"      
           and lower(encounterdiagnosistext) like "%monkeypox%" 
           and lower(encounterdiagnosistext) like "%mpox%" 
           and lower(encounterdiagnosistext) like "%m-pox%"
           and lower(encounterdiagnosistext) like "%mpx%"
           and lower(encounterdiagnosistext) not like "%accidental%"           
           and encounterdate > 2022-01-01                      
           """)        
        .groupBy("patientuid","encounterdiagnosiscode")        
        .agg(
            F.count("encounterdiagnosiscode").alias("count"),
            F.collect_set("encounterdiagnosistext").alias("string_monkeypox")            
        )
                        
        .orderBy(F.desc("count"))
        
)



# COMMAND ----------

# MAGIC %md # Visit

# COMMAND ----------

display(
    visit.where("""                    
                encountertypecode like "B04" 
                or encountertypecode like "359811007"
                or encountertypecode like "359814004"
                or encountertypecode like "59774002"
                or encountertypecode like "1003839"
                or encountertypecode like "1004340"
                or encountertypecode like "05900"
                or encountertypecode like "059%"                              
                or encountertypecode like "414015000"  
                or encountertypecode like "B0869"
                or encountertypecode like "B0860"
                or encountertypecode like "59010"
                
                or lower(encountertypetext) like "%monkeypox%"
                or lower(encountertypetext) like "%monkypox%"
                or lower(encountertypetext) like "%monkey pox%"
                or lower(encountertypetext) like "%monkepox pox%"
                or lower(encountertypetext) like "%monky pox%"
                or lower(encountertypetext) like "%monkey-pox%"
                or lower(encountertypetext) like "%monke-pox%"
                or lower(encountertypetext) like "%monky-pox%"
                or lower(encountertypetext) like "%orthopox%"
                or lower(encountertypetext) like "%orthopoxvirus%"
                or lower(encountertypetext) like "%parapoxvirus%"           
                or lower(encountertypetext) like "%monk%"           
                or lower(encountertypetext) like "%ortho pox%"
                or lower(encountertypetext) like "%poxvi%"      
                or lower(encountertypetext) like "%monkeypox%" 
                or lower(encountertypetext) like "%mpox%" 
                or lower(encountertypetext) like "%m-pox%"
                or lower(encountertypetext) like "%mpx%"
                and lower(encountertypetext) not like "%accidental%"  

                or lower(note) like "%monkeypox%"
                or lower(note) like "%monkypox%"
                or lower(note) like "%monkey pox%"
                or lower(note) like "%monkepox pox%"
                or lower(note) like "%monky pox%"
                or lower(note) like "%monkey-pox%"
                or lower(note) like "%monke-pox%"
                or lower(note) like "%monky-pox%"
                or lower(note) like "%orthopox%"
                or lower(note) like "%orthopoxvirus%"
                or lower(note) like "%parapoxvirus%"           
                or lower(note) like "%monk%"           
                or lower(note) like "%ortho pox%"
                or lower(note) like "%poxvi%"      
                or lower(note) like "%monkeypox%" 
                or lower(note) like "%mpox%" 
                or lower(note) like "%m-pox%"
                or lower(note) like "%mpx%"
                and lower(note) not like "%accidental%"   
                and encounterstartdate > '2022-01-01'                   
           """)
           
)
 

# COMMAND ----------

# MAGIC %md #patientsocialhistoryobservation

# COMMAND ----------

display(
    pxSocHis
    .where("""
                   lower(socialhistorytypetext) like "%monkeypox%"
                and lower(socialhistorytypetext) like "%monkypox%"
                and lower(socialhistorytypetext) like "%monkey pox%"
                and lower(socialhistorytypetext) like "%monkepox pox%"
                and lower(socialhistorytypetext) like "%monky pox%"
                and lower(socialhistorytypetext) like "%monkey-pox%"
                and lower(socialhistorytypetext) like "%monke-pox%"
                and lower(socialhistorytypetext) like "%monky-pox%"
                and lower(socialhistorytypetext) like "%orthopox%"
                and lower(socialhistorytypetext) like "%orthopoxvirus%"
                and lower(socialhistorytypetext) like "%parapoxvirus%"           
                and lower(socialhistorytypetext) like "%monk%"           
                and lower(socialhistorytypetext) like "%ortho pox%"
                and lower(socialhistorytypetext) like "%poxvi%"      
                and lower(socialhistorytypetext) like "%monkeypox%" 
                and lower(socialhistorytypetext) like "%mpox%" 
                and lower(socialhistorytypetext) like "%m-pox%"
                and lower(socialhistorytypetext) like "%mpx%"
                and lower(socialhistorytypetext) not like "%accidental%" 
                and lower(socialhistorytypetext) not like "%20 years%"   
           
           """).sort("documentationdate", ascending = False)
)

# COMMAND ----------

# MAGIC %md #patientprocedureobservation

# COMMAND ----------

time_variable = "resultdate"
note_variables = ['practicedescription', 'resultorderdescription', 'proceduretext', 'methodcodetext', 'observationdescription']

treated_note = pxResultObs.select("*", cleanDate(time_variable).alias("clean_date"), deleteMarkup("note").alias("cleaned_note"))
mpox_notes_patientResObs = find_partial_matches(treated_note, words_to_match_mpox, note_variables)

display(mpox_notes_patientResObs)


# COMMAND ----------

# continue here

display(
    pxResultObs
    .where("""   
            practicecode like "B04" 
            or practicecode like "359811007"
            or practicecode like "359814004"
            or practicecode like "59774002"
            or practicecode like "1003839"
            or practicecode like "1004340"
            or practicecode like "05900"
            or practicecode like "059"                              
            or practicecode like "414015000"  
            or practicecode like "B0869"
            or practicecode like "B0860"
            or practicecode like "59010"            

            --or lower(practicedescription) like "%monkeypox%"
            --or lower(practicedescription) like "%monkypox%"
            --or lower(practicedescription) like "%monkey pox%"
            --or lower(practicedescription) like "%monkepox pox%"
            --or lower(practicedescription) like "%monky pox%"
            --or lower(practicedescription) like "%monkey-pox%"
            --or lower(practicedescription) like "%monke-pox%"
            --or lower(practicedescription) like "%monky-pox%"
            --or lower(practicedescription) like "%orthopox%"
            --or lower(practicedescription) like "%orthopoxvirus%"
            --or lower(practicedescription) like "%parapoxvirus%"           
            --or lower(practicedescription) like "%monk%"           
            --or lower(practicedescription) like "%ortho pox%"
            --or lower(practicedescription) like "%poxvi%"      
            --or lower(practicedescription) like "%monkeypox%" 
            --or lower(practicedescription) like "%mpox%" 
            --or lower(practicedescription) like "%m-pox%"
            --or lower(practicedescription) like "%mpx%"


            or lower(resultorderdescription) like "%monkeypox%"
            or lower(resultorderdescription) like "%monkypox%"
            or lower(resultorderdescription) like "%monkey pox%"
            or lower(resultorderdescription) like "%monkepox pox%"
            or lower(resultorderdescription) like "%monky pox%"
            or lower(resultorderdescription) like "%monkey-pox%"
            or lower(resultorderdescription) like "%monke-pox%"
            or lower(resultorderdescription) like "%monky-pox%"
            or lower(resultorderdescription) like "%orthopox%"
            or lower(resultorderdescription) like "%orthopoxvirus%"
            or lower(resultorderdescription) like "%parapoxvirus%"           
            or lower(resultorderdescription) like "%monk%"           
            or lower(resultorderdescription) like "%ortho pox%"
            or lower(resultorderdescription) like "%poxvi%"      
            or lower(resultorderdescription) like "%monkeypox%" 
            or lower(resultorderdescription) like "%mpox%" 
            or lower(resultorderdescription) like "%m-pox%"
            --or lower(resultorderdescription) like "%mpx%" 

            or lower(proceduretext) like "%monkeypox%"
            or lower(proceduretext) like "%monkypox%"
            or lower(proceduretext) like "%monkey pox%"
            or lower(proceduretext) like "%monkepox pox%"
            or lower(proceduretext) like "%monky pox%"
            or lower(proceduretext) like "%monkey-pox%"
            or lower(proceduretext) like "%monke-pox%"
            or lower(proceduretext) like "%monky-pox%"
            or lower(proceduretext) like "%orthopox%"
            or lower(proceduretext) like "%orthopoxvirus%"
            or lower(proceduretext) like "%parapoxvirus%"           
            or lower(proceduretext) like "%monk%"           
            or lower(proceduretext) like "%ortho pox%"
            or lower(proceduretext) like "%poxvi%"      
            or lower(proceduretext) like "%monkeypox%" 
            or lower(proceduretext) like "%mpox%" 
            or lower(proceduretext) like "%m-pox%"
            or lower(proceduretext) like "%mpx%" 


                   -- it is hard to know what method code text is   
           or methodcodetext like "%monk%"
           or methodcodetext like "%ortho pox"
           or methodcodetext like "%poxvi%"           
           or methodcodetext like "B04"
           or methodcodetext like "359811007"
           or methodcodetext like "359814004"           
           or methodcodetext like "59774002"
           or methodcodetext like "05900"
           or methodcodetext like "O5900"


           or lower(observationdescription) like "%monkeypox%"
           or lower(observationdescription) like "%monkypox%"
           or lower(observationdescription) like "%monkey pox%"
           or lower(observationdescription) like "%monkepox pox%"
           or lower(observationdescription) like "%monky pox%"
           or lower(observationdescription) like "%monkey-pox%"
           or lower(observationdescription) like "%monke-pox%"
           or lower(observationdescription) like "%monky-pox%"
           or lower(observationdescription) like "%orthopox%"
           or lower(observationdescription) like "%orthopoxvirus%"
           or lower(observationdescription) like "%parapoxvirus%"           
           or lower(observationdescription) like "%monk%"           
           or lower(observationdescription) like "%ortho pox%"
           or lower(observationdescription) like "%poxvi%"      
           or lower(observationdescription) like "%monkeypox%" 
           or lower(observationdescription) like "%mpox%" 
           or lower(observationdescription) like "%m-pox%"
           or lower(observationdescription) like "%mpx%" 

           
      
           """).sort("resultdate", ascending = False)
)

# COMMAND ----------

# continue here

display(
    pxResultObs
    .where("""   
                  
               lower(practicedescription) like "%monkeypox%"
            or lower(practicedescription) like "%monkypox%"
            or lower(practicedescription) like "%monkey pox%"
            or lower(practicedescription) like "%monkepox pox%"
            or lower(practicedescription) like "%monky pox%"
            or lower(practicedescription) like "%monkey-pox%"
            or lower(practicedescription) like "%monke-pox%"
            or lower(practicedescription) like "%monky-pox%"
            or lower(practicedescription) like "%orthopox%"
            or lower(practicedescription) like "%orthopoxvirus%"
            or lower(practicedescription) like "%parapoxvirus%"           
          
            or lower(practicedescription) like "%ortho pox%"
            or lower(practicedescription) like "%poxvi%"      
            or lower(practicedescription) like "%monkeypox%" 
            or lower(practicedescription) like "%mpox%" 
            or lower(practicedescription) like "%m-pox%"
            or lower(practicedescription) like "%mpx%"


            or lower(resultorderdescription) like "%monkeypox%"
            or lower(resultorderdescription) like "%monkypox%"
            or lower(resultorderdescription) like "%monkey pox%"
            or lower(resultorderdescription) like "%monkepox pox%"
            or lower(resultorderdescription) like "%monky pox%"
            or lower(resultorderdescription) like "%monkey-pox%"
            or lower(resultorderdescription) like "%monke-pox%"
            or lower(resultorderdescription) like "%monky-pox%"
            or lower(resultorderdescription) like "%orthopox%"
            or lower(resultorderdescription) like "%orthopoxvirus%"
            or lower(resultorderdescription) like "%parapoxvirus%"           
          
            or lower(resultorderdescription) like "%ortho pox%"
            or lower(resultorderdescription) like "%poxvi%"      
            or lower(resultorderdescription) like "%monkeypox%" 
            or lower(resultorderdescription) like "%mpox%" 
            or lower(resultorderdescription) like "%m-pox%"
            or lower(resultorderdescription) like "%mpx%" 

            or lower(proceduretext) like "%monkeypox%"
            or lower(proceduretext) like "%monkypox%"
            or lower(proceduretext) like "%monkey pox%"
            or lower(proceduretext) like "%monkepox pox%"
            or lower(proceduretext) like "%monky pox%"
            or lower(proceduretext) like "%monkey-pox%"
            or lower(proceduretext) like "%monke-pox%"
            or lower(proceduretext) like "%monky-pox%"
            or lower(proceduretext) like "%orthopox%"
            or lower(proceduretext) like "%orthopoxvirus%"
            or lower(proceduretext) like "%parapoxvirus%"           
      
            or lower(proceduretext) like "%ortho pox%"
            or lower(proceduretext) like "%poxvi%"      
            or lower(proceduretext) like "%monkeypox%" 
            or lower(proceduretext) like "%mpox%" 
            or lower(proceduretext) like "%m-pox%"
            or lower(proceduretext) like "%mpx%" 


            or lower(observationdescription) like "%monkeypox%"
           or lower(observationdescription) like "%monkypox%"
           or lower(observationdescription) like "%monkey pox%"
           or lower(observationdescription) like "%monkepox pox%"
           or lower(observationdescription) like "%monky pox%"
           or lower(observationdescription) like "%monkey-pox%"
           or lower(observationdescription) like "%monke-pox%"
           or lower(observationdescription) like "%monky-pox%"
           or lower(observationdescription) like "%orthopox%"
           or lower(observationdescription) like "%orthopoxvirus%"
           or lower(observationdescription) like "%parapoxvirus%"           
                   
           or lower(observationdescription) like "%ortho pox%"
           or lower(observationdescription) like "%poxvi%"      
           or lower(observationdescription) like "%monkeypox%" 
           or lower(observationdescription) like "%mpox%" 
           or lower(observationdescription) like "%m-pox%"
           or lower(observationdescription) like "%mpx%" 

           """).sort("resultdate", ascending = False)
)

# COMMAND ----------

pxProcedure.select("*", F.to_date(F.col("effectivedate"),"MM-dd-yyyy"))

# COMMAND ----------

# MAGIC %md #patientprocedure

# COMMAND ----------

display(
    pxProcedure.select("*", F.to_date(F.date_format(F.col("effectivedate"),"yyyy-MM-dd"), "yyyy-MM-dd").alias("dt"))
                .where("""
                   procedurecode like "B04" 
                or procedurecode like "359811007"
                or procedurecode like "359814004"
                or procedurecode like "59774002"
                or procedurecode like "1003839"
                or procedurecode like "1004340"
                or procedurecode like "05900"
                or procedurecode like "059%"                              
                or procedurecode like "414015000"  
                or procedurecode like "B0869"
                or procedurecode like "B0860"
                or procedurecode like "59010"

                or lower(proceduredescription) like "%monkeypox%"
                or lower(proceduredescription) like "%monkypox%"
                or lower(proceduredescription) like "%monkey pox%"
                or lower(proceduredescription) like "%monkepox pox%"
                or lower(proceduredescription) like "%monky pox%"
                or lower(proceduredescription) like "%monkey-pox%"
                or lower(proceduredescription) like "%monke-pox%"
                or lower(proceduredescription) like "%monky-pox%"
                or lower(proceduredescription) like "%orthopox%"
                or lower(proceduredescription) like "%orthopoxvirus%"
                or lower(proceduredescription) like "%parapoxvirus%"           
                or lower(proceduredescription) like "%monk%"           
                or lower(proceduredescription) like "%ortho pox%"
                or lower(proceduredescription) like "%poxvi%"      
                or lower(proceduredescription) like "%monkeypox%" 
                or lower(proceduredescription) like "%mpox%" 
                or lower(proceduredescription) like "%m-pox%"
                or lower(proceduredescription) like "%mpx%"
                and lower(proceduredescription) not like "%accidental%"  

                or lower(procedurenote) like "%monkeypox%"
                or lower(procedurenote) like "%monkypox%"
                or lower(procedurenote) like "%monkey pox%"
                or lower(procedurenote) like "%monkepox pox%"
                or lower(procedurenote) like "%monky pox%"
                or lower(procedurenote) like "%monkey-pox%"
                or lower(procedurenote) like "%monke-pox%"
                or lower(procedurenote) like "%monky-pox%"
                or lower(procedurenote) like "%orthopox%"
                or lower(procedurenote) like "%orthopoxvirus%"
                or lower(procedurenote) like "%parapoxvirus%"           
                or lower(procedurenote) like "%monk%"           
                or lower(procedurenote) like "%ortho pox%"
                or lower(procedurenote) like "%poxvi%"      
                or lower(procedurenote) like "%monkeypox%" 
                or lower(procedurenote) like "%mpox%" 
                or lower(procedurenote) like "%m-pox%"
                or lower(procedurenote) like "%mpx%"
                and lower(procedurenote) not like "%accidental%" 
                and dt > '2022-01-01' 
         
           """).sort("dt", ascending = False)
)

# COMMAND ----------

# MAGIC %md # patientproblem

# COMMAND ----------

display(
    pxProm
    .where("""
                problemcode like "B04" 
            or problemcode like "359811007"
            or problemcode like "359814004"
            or problemcode like "59774002"
            or problemcode like "1003839"
            or problemcode like "1004340"
            or problemcode like "05900"
            or problemcode like "059%"                              
            or problemcode like "414015000"  
            or problemcode like "B0869"
            or problemcode like "B0860"
            or problemcode like "59010"

            or problemstatuscode like "B04" 
            or problemstatuscode like "359811007"
            or problemstatuscode like "359814004"
            or problemstatuscode like "59774002"
            or problemstatuscode like "1003839"
            or problemstatuscode like "1004340"
            or problemstatuscode like "05900"
            or problemstatuscode like "059%"                              
            or problemstatuscode like "414015000"  
            or problemstatuscode like "B0869"
            or problemstatuscode like "B0860"
            or problemstatuscode like "59010"
                   
            or lower(problemtext) like "%monkeypox%"
            or lower(problemtext) like "%monkypox%"
            or lower(problemtext) like "%monkey pox%"
            or lower(problemtext) like "%monkepox pox%"
            or lower(problemtext) like "%monky pox%"
            or lower(problemtext) like "%monkey-pox%"
            or lower(problemtext) like "%monke-pox%"
            or lower(problemtext) like "%monky-pox%"
            or lower(problemtext) like "%orthopox%"
            or lower(problemtext) like "%orthopoxvirus%"
            or lower(problemtext) like "%parapoxvirus%"           
            or lower(problemtext) like "%monk%"           
            or lower(problemtext) like "%ortho pox%"
            or lower(problemtext) like "%poxvi%"      
            or lower(problemtext) like "%monkeypox%" 
            or lower(problemtext) like "%mpox%" 
            or lower(problemtext) like "%m-pox%"
            or lower(problemtext) like "%mpx%"

            and lower(problemcomment) like "%monkeypox%"
            and lower(problemcomment) like "%monkypox%"
            and lower(problemcomment) like "%monkey pox%"
            and lower(problemcomment) like "%monkepox pox%"
            and lower(problemcomment) like "%monky pox%"
            and lower(problemcomment) like "%monkey-pox%"
            and lower(problemcomment) like "%monke-pox%"
            and lower(problemcomment) like "%monky-pox%"
            and lower(problemcomment) like "%orthopox%"
            and lower(problemcomment) like "%orthopoxvirus%"
            and lower(problemcomment) like "%parapoxvirus%"           
            and lower(problemcomment) like "%monk%"           
            and lower(problemcomment) like "%ortho pox%"
            and lower(problemcomment) like "%poxvi%"      
            and lower(problemcomment) like "%monkeypox%" 
            and lower(problemcomment) like "%mpox%" 
            and lower(problemcomment) like "%m-pox%"
            and lower(problemcomment) like "%mpx%"
            and lower(problemcomment) not like "%monkey bar%"
 
           """).sort("documentationdate", ascending = False)
)
    

# COMMAND ----------

                   encountertypecode like "B04" 
                or encountertypecode like "359811007"
                or encountertypecode like "359814004"
                or encountertypecode like "59774002"
                or encountertypecode like "1003839"
                or encountertypecode like "1004340"
                or encountertypecode like "05900"
                or encountertypecode like "059%"                              
                or encountertypecode like "414015000"  
                or encountertypecode like "B0869"
                or encountertypecode like "B0860"
                or encountertypecode like "59010"
                
                or lower(encountertypetext) like "%monkeypox%"
                or lower(encountertypetext) like "%monkypox%"
                or lower(encountertypetext) like "%monkey pox%"
                or lower(encountertypetext) like "%monkepox pox%"
                or lower(encountertypetext) like "%monky pox%"
                or lower(encountertypetext) like "%monkey-pox%"
                or lower(encountertypetext) like "%monke-pox%"
                or lower(encountertypetext) like "%monky-pox%"
                or lower(encountertypetext) like "%orthopox%"
                or lower(encountertypetext) like "%orthopoxvirus%"
                or lower(encountertypetext) like "%parapoxvirus%"           
                or lower(encountertypetext) like "%monk%"           
                or lower(encountertypetext) like "%ortho pox%"
                or lower(encountertypetext) like "%poxvi%"      
                or lower(encountertypetext) like "%monkeypox%" 
                or lower(encountertypetext) like "%mpox%" 
                or lower(encountertypetext) like "%m-pox%"
                or lower(encountertypetext) like "%mpx%"
                and lower(encountertypetext) not like "%accidental%"  

                or lower(note) like "%monkeypox%"
                or lower(note) like "%monkypox%"
                or lower(note) like "%monkey pox%"
                or lower(note) like "%monkepox pox%"
                or lower(note) like "%monky pox%"
                or lower(note) like "%monkey-pox%"
                or lower(note) like "%monke-pox%"
                or lower(note) like "%monky-pox%"
                or lower(note) like "%orthopox%"
                or lower(note) like "%orthopoxvirus%"
                or lower(note) like "%parapoxvirus%"           
                or lower(note) like "%monk%"           
                or lower(note) like "%ortho pox%"
                or lower(note) like "%poxvi%"      
                or lower(note) like "%monkeypox%" 
                or lower(note) like "%mpox%" 
                or lower(note) like "%m-pox%"
                or lower(note) like "%mpx%"
                and lower(note) not like "%accidental%"   
                and encounterstartdate > 2022-01-01             

# COMMAND ----------

# MAGIC %md # patientnoteresultsobservation

# COMMAND ----------

display(pxNoteResObs)

# COMMAND ----------

display(
    pxNoteResObs.select("*", deleteMarkup("note").alias("cleaned_note"))        
    .where("""              

               lower(note) like "%monkeypox%"
            or lower(note) like "%monkypox%"
            or lower(note) like "%monkey pox%"
            or lower(note) like "%monkepox pox%"
            or lower(note) like "%monky pox%"
            or lower(note) like "%monkey-pox%"
            or lower(note) like "%monke-pox%"
            or lower(note) like "%monky-pox%"
            or lower(note) like "%orthopox%"
            or lower(note) like "%orthopoxvirus%"
            or lower(note) like "%parapoxvirus%"           
            or lower(note) like "%monk%"           
            or lower(note) like "%ortho pox%"
            or lower(note) like "%poxvi%"      
            or lower(note) like "%monkeypox%" 
            or lower(note) like "%mpox%" 
            or lower(note) like "%m-pox%"
            or lower(note) like "%mpx%"
            and lower(note) not like "%accidental%"  
            and lower(note) not like "%monkey bar%"  
            --and encounterdate > 2022-04-01       
           """
           ).sort("encounterdate", ascending = False)
)

#588

# COMMAND ----------

display(
    pxNoteResObs.select("*", deleteMarkup("note").alias("cleaned_note"))        
    .where("""              

               lower(note) like "%monkeypox%"
            or lower(note) like "%monkypox%"
            or lower(note) like "%monkey pox%"
            or lower(note) like "%monkepox pox%"
            or lower(note) like "%monky pox%"
            or lower(note) like "%monkey-pox%"
            or lower(note) like "%monke-pox%"
            or lower(note) like "%monky-pox%"
            or lower(note) like "%orthopox%"
            or lower(note) like "%orthopoxvirus%"
            or lower(note) like "%parapoxvirus%"           
            or lower(note) like "%monk%"           
            or lower(note) like "%ortho pox%"
            or lower(note) like "%poxvi%"      
            or lower(note) like "%monkeypox%" 
            or lower(note) like "%mpox%" 
            or lower(note) like "%m-pox%"
            or lower(note) like "%mpx%"
            and lower(note) not like "%accidental%"  
            and lower(note) not like "%monkey bar%"  
            --and encounterdate > 2022-04-01       
           """
           ).sort("encounterdate", ascending = False)
    .groupBy("practiceid")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

# COMMAND ----------

# MAGIC %md # patientnoteprocedure

# COMMAND ----------

display(
    pxNoteProc
    .where("""
           
           lower(note) like "%monkeypox%"
            or lower(note) like "%monkypox%"
            or lower(note) like "%monkey pox%"
            or lower(note) like "%monkepox pox%"
            or lower(note) like "%monky pox%"
            or lower(note) like "%monkey-pox%"
            or lower(note) like "%monke-pox%"
            or lower(note) like "%monky-pox%"
            or lower(note) like "%orthopox%"
            or lower(note) like "%orthopoxvirus%"
            or lower(note) like "%parapoxvirus%"           
            or lower(note) like "%monk%"           
            or lower(note) like "%ortho pox%"
            or lower(note) like "%poxvi%"      
            or lower(note) like "%monkeypox%" 
            or lower(note) like "%mpox%" 
            or lower(note) like "%m-pox%"
            or lower(note) like "%mpx%"
                      
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

               lower(sectionname) like "%monkeypox%"
            or lower(sectionname) like "%monkypox%"
            or lower(sectionname) like "%monkey pox%"
            or lower(sectionname) like "%monkepox pox%"
            or lower(sectionname) like "%monky pox%"
            or lower(sectionname) like "%monkey-pox%"
            or lower(sectionname) like "%monke-pox%"
            or lower(sectionname) like "%monky-pox%"
            or lower(sectionname) like "%orthopox%"
            or lower(sectionname) like "%orthopoxvirus%"
            or lower(sectionname) like "%parapoxvirus%"           
            or lower(sectionname) like "%monk%"           
            or lower(sectionname) like "%ortho pox%"
            or lower(sectionname) like "%poxvi%"      
            or lower(sectionname) like "%monkeypox%" 
            or lower(sectionname) like "%mpox%" 
            or lower(sectionname) like "%m-pox%"
            or lower(sectionname) like "%mpx%"

            or lower(note) like "%monkeypox%"
            or lower(note) like "%monkypox%"
            or lower(note) like "%monkey pox%"
            or lower(note) like "%monkepox pox%"
            or lower(note) like "%monky pox%"
            or lower(note) like "%monkey-pox%"
            or lower(note) like "%monke-pox%"
            or lower(note) like "%monky-pox%"
            or lower(note) like "%orthopox%"
            or lower(note) like "%orthopoxvirus%"
            or lower(note) like "%parapoxvirus%"           
            or lower(note) like "%monk%"           
            or lower(note) like "%ortho pox%"
            or lower(note) like "%poxvi%"      
            or lower(note) like "%monkeypox%" 
            or lower(note) like "%mpox%" 
            or lower(note) like "%m-pox%"
            or lower(note) like "%mpx%" 
           
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
          
           or lower(description) like "%monkeypox%"
           or lower(description) like "%monkypox%"
           or lower(description) like "%monkey pox%"
           or lower(description) like "%monkepox pox%"
           or lower(description) like "%monky pox%"
           or lower(description) like "%monkey-pox%"
           or lower(description) like "%monke-pox%"
           or lower(description) like "%monky-pox%"
           or lower(description) like "%orthopox%"
           or lower(description) like "%orthopoxvirus%"
           or lower(description) like "%parapoxvirus%"           
           or lower(description) like "%monk%"           
           or lower(description) like "%ortho pox%"
           or lower(description) like "%poxvi%"      
           or lower(description) like "%monkeypox%" 
           or lower(description) like "%mpox%" 
           or lower(description) like "%m-pox%"
           or lower(description) like "%mpx%" 

           
           """).sort("orderdate", ascending = False)
)

# COMMAND ----------

display(labOrder    
    .where("""
              lower(description) like "%monkeypox%"
           or lower(description) like "%monkypox%"
           or lower(description) like "%monkey pox%"
           or lower(description) like "%monkepox pox%"
           or lower(description) like "%monky pox%"
           or lower(description) like "%monkey-pox%"
           or lower(description) like "%monke-pox%"
           or lower(description) like "%monky-pox%"
           or lower(description) like "%orthopox%"
           or lower(description) like "%orthopoxvirus%"
           or lower(description) like "%parapoxvirus%"           
           or lower(description) like "%monk%"           
           or lower(description) like "%ortho pox%"
           or lower(description) like "%poxvi%"      
           or lower(description) like "%monkeypox%" 
           or lower(description) like "%mpox%" 
           or lower(description) like "%m-pox%"
           or lower(description) like "%mpx%" 
           and orderdate > '2020-05-22'""")        
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

# MAGIC %md # Patient imunization 

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
              lower(note) like "%B04%"
           or lower(note) like "%359811007%"
           or lower(note) like "%359814004%"
           or lower(note) like "%59774002%"
           or lower(note) like "%05900%"
           or lower(note) like "%O5900%"
           or lower(note) like "%monke%"           
           or lower(note) like "%ortho pox"
           or lower(note) like "%poxvi%" 
           or lower(note) like "O5909"  
           or lower(note) like "O5901"

           or lower(note) like "%monkeypox%"
           or lower(note) like "%monkypox%"
           or lower(note) like "%monkey pox%"
           or lower(note) like "%monkepox pox%"
           or lower(note) like "%monky pox%"
           or lower(note) like "%monkey-pox%"
           or lower(note) like "%monke-pox%"
           or lower(note) like "%monky-pox%"
           or lower(note) like "%orthopox%"
           or lower(note) like "%orthopoxvirus%"
           or lower(note) like "%parapoxvirus%"           
           or lower(note) like "%monke%"           
           or lower(note) like "%ortho pox%"
           or lower(note) like "%poxvi%"      
           or lower(note) like "%monkeypox%" 
           or lower(note) like "%mpox%" 
           or lower(note) like "%m-pox%"
           or lower(note) like "%mpx%" 
           
           or group1 like "%B04%"
           or group1 like "%359811007%"
           or group1 like "%359814004%"
           or group1 like "%59774002%"
           or group1 like "%05900%"
           or group1 like "%O5900%"
           or group1 like "%monke%"           
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
           or group2 like "%monke%"           
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
           or group3 like "%monke%"           
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
           or group4 like "%monke%"           
           or group4 like "%ortho pox"
           or group4 like "%poxvi%"
           or group4 like "O5909"  
           or group4 like "O5901"
                      
     """).sort("encounterdate", ascending = False)   
)



# COMMAND ----------

display(pxResObs)

# COMMAND ----------

# MAGIC %md # Testing Pretrained models

# COMMAND ----------

# MAGIC %md Testing the following task
# MAGIC
# MAGIC 1. Zero shot classification
# MAGIC 2. Name entity recognition
# MAGIC 3. Sentiment analysis
# MAGIC 4. Text classification

# COMMAND ----------

# sample notes

note1 = ["Doubt monkey pox, or zoster.|Hope is not mrsa|Cover with keflex and rto 3-4 d if not responding|9/16/22  Small infection is gone.  Lump itself is not tender now, but same size.  We will refer to surgery for their opinion"]

note2 = ["""RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs; CARDIOVASCULAR: normal rate; rhythm is regular; no systolic murmur; GASTROINTESTINAL: nontender; normal bowel sounds; LYMPHATICS: no adenopathy in cervical, supraclavicular, axillary, or inguinal regions; BREAST/INTEGUMENT: no rashes or lesions; MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone; NEUROLOGIC: appropriate for age; Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius"""]

sequences = note1 + note2

# COMMAND ----------

# MAGIC %md ## Clinical bert 

# COMMAND ----------

cleaned = (
    pxResObs.select("patientuid","note")
    .withColumn("clean_note_RTF", 
                regexp_replace(F.col("note"), r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","")
                )
    .withColumn("clean_note_XLM", 
                regexp_replace(F.col("clean_note_RTF"), r"<[^>]*>|&[^;]+;", "")
                )
    )
    
    #.where("patientuid in ('231b73b2-bd61-4a7e-ba78-cb28dae7c8da', '75e94e91-7555-4838-aad0-ab8ff7390e9a', '79a58e93-dc05-4372-8e2e-b17276dc4a77')")
    

display(cleaned)
    


# COMMAND ----------

from transformers import AutoModel, AutoTokenizer

checkpoint = "emilyalsentzer/Bio_ClinicalBERT"
tokenizer = AutoTokenizer.from_pretrained(checkpoint)

model = AutoModel.from_pretrained(checkpoint)

model_inputs = tokenizer(sequences)

tokens = tokenizer(sequences, padding=True, truncation=True, return_tensors="pt")
output = model(**tokens)

tokens

# COMMAND ----------

#print(tokenizer.decode(model_inputs["input_ids"]))
print(tokenizer.decode(ids))

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification")

classifier(
    note1,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer)
concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer, grouped_entities = True)

entities = concept_extraction(note1)

entities

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer)
concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer, grouped_entities = True)

entities = concept_extraction(note2)

entities

# COMMAND ----------

# MAGIC %md ### Testing for alternative zero shot (facebook/bart-large-mnli)

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification")

classifier(
    note1,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

classifier(
    note2,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

# sentiment analysis

model_name = "emilyalsentzer/Bio_ClinicalBERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note1,    
)

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note2,    
)

# COMMAND ----------

classifier = pipeline("text-classification", model = "emilyalsentzer/Bio_ClinicalBERT")

classifier(
    note1,    
)

# COMMAND ----------

classifier(
    note2,    
)

# COMMAND ----------

# MAGIC %md ## GatorTron-0G

# COMMAND ----------

from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig

MODEL_NAME = 'AshtonIsNotHere/GatorTron-OG'
num_labels = 2

config = AutoConfig.from_pretrained(MODEL_NAME, num_labels=num_labels)
model  =  AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, config=config)
tokenizer  =  AutoTokenizer.from_pretrained(MODEL_NAME)




# COMMAND ----------

featrure_extraction = pipeline("ner", model=model, tokenizer = tokenizer, grouped_entities = True)

classifier(
    note1,    
)

# COMMAND ----------

featrure_extraction = pipeline("ner", model=model, tokenizer = tokenizer)

classifier(
    note2,    
)

# COMMAND ----------

model_name = "AshtonIsNotHere/GatorTron-OG"
classifier = pipeline("ner", model=model_name, tokenizer=tokenizer)


classifier(
    note2,    
)

# COMMAND ----------

#zero shot

#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification",  model=model, tokenizer = tokenizer)

classifier(
    note1,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

classifier = pipeline("sentiment-analysis",  model=model, tokenizer = tokenizer)

classifier(
    note1,    
)

# COMMAND ----------

# MAGIC %md ## PHS-BERT

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("publichealthsurveillance/PHS-BERT")
model = AutoModel.from_pretrained("publichealthsurveillance/PHS-BERT")

# COMMAND ----------

model_name = "publichealthsurveillance/PHS-BERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note1,    
)

# COMMAND ----------

classifier(
    note2,    
)

# COMMAND ----------

# MAGIC %md ## DIstilled BERT

# COMMAND ----------


