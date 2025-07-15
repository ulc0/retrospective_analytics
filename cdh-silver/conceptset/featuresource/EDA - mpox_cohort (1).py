# Databricks notebook source
import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, explode
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
spark.sql("SHOW DATABASES").show(truncate=False)

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

# MAGIC %run ./0000-utils-high

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

mpox_codes = ( [
            "B04", "359811007", "359814004", "59774002", "1003839",
            "1004340", "05900", "059", "414015000",  "B0869", "B0860" ,
           # "59010"
           ] 
          )

STI_visit_codes = (
    [
        'A510',	'A511',	'A512',	'A5131',	'A5132',	'A5139',	'A5141',	'A5142',	'A5143',	'A5144',	'A5145',	'A5146',	'A5149',	'A515',	'A519',	'A5200',	'A5201',	'A5202',	'A5203',	'A5204',	'A5205',	'A5206',	'A5209',	'A5210',	'A5211',	'A5212',	'A5213',	'A5214',	'A5215',	'A5216',	'A5217',	'A5219',	'A522',	'A523',	'A5271',	'A5272',	'A5273',	'A5274',	'A5275',	'A5276',	'A5277',	'A5278',	'A5279',	'A528',	'A529',	'A530',	'A539',	'A5400',	'A5401',	'A5402',	'A5403',	'A5409',	'A541',	'A5421',	'A5422',	'A5423',	'A5424',	'A5429',	'A5440',	'A5441',	'A5442',	'A5443',	'A5449',	'A545',	'A546',	'A5481',	'A5482',	'A5483',	'A5484',	'A5485',	'A5489',	'A549',	'A55',	'A5600',	'A5601',	'A5602',	'A5609',	'A5611',	'A5619',	'A562',	'A563',	'A564',	'A568',	'A57',	'A58',	'A5900',	'A5901',	'A5902',	'A5903',	'A5909',	'A598',	'A599',	'A6000',	'A6001',	'A6002',	'A6003',	'A6004',	'A6009',	'A601',	'A609',	'A630',	'A638',	'A64',	'B0081',	'B150',	'B159',	'B160',	'B161',	'B162',	'B169',	'B170',	'B1710',	'B1711',	'B172',	'B178',	'B179',	'B180',	'B181',	'B182',	'B188',	'B189',	'B190',	'B1910',	'B1911',	'B1920',	'B1921',	'B199',	'B20',	'B251',	'B2681',	'B581',	'K7010',	'K7011',	'Z21'

    ]
)

NYC_STI = ( [
            'Z113',	'Z114',	'Z202',	'Z206'
           ] 
          )

CCRS_econ= ( [ # this would go after flagging the STI visits
            'Z550',	'Z551',	'Z552',	'Z553',	'Z554',	'Z555',	'Z558',	'Z559',	'Z560',	'Z561',	'Z562',	'Z563',	'Z564',	'Z565',	'Z566',	'Z5681',	'Z5682',	'Z5689',	'Z569',	'Z570',	'Z571',	'Z572',	'Z5731',	'Z5739',	'Z574',	'Z575',	'Z576',	'Z577',	'Z578',	'Z579',	'Z586',	'Z590',	'Z5900',	'Z5901',	'Z5902',	'Z591',	'Z592',	'Z593',	'Z594',	'Z5941',	'Z5948',	'Z595',	'Z596',	'Z597',	'Z598',	'Z59811',	'Z59812',	'Z59819',	'Z5982',	'Z5986',	'Z5987',	'Z5989',	'Z599',	'Z600',	'Z602',	'Z603',	'Z604',	'Z605',	'Z608',	'Z609',	'Z620',	'Z621',	'Z6221',	'Z6222',	'Z6229',	'Z623',	'Z626',	'Z62810',	'Z62811',	'Z62812',	'Z62813',	'Z62819',	'Z62820',	'Z62821',	'Z62822',	'Z62890',	'Z62891',	'Z62898',	'Z629',	'Z630',	'Z631',	'Z6331',	'Z6332',	'Z634',	'Z635',	'Z636',	'Z6371',	'Z6372',	'Z6379',	'Z638',	'Z639',	'Z640',	'Z641',	'Z644',	'Z650',	'Z651',	'Z652',	'Z653',	'Z654',	'Z655',	'Z658',	'Z659',
           ] 
          )

all_codes = mpox_codes+STI_visit_codes+NYC_STI


study_period_starts = '2022-04-01'
study_period_ends = '2023-01-01'

# COMMAND ----------


visit  = spark.table("cdh_abfm_phi.visit") # 1) diagnosis
#visitDiag = spark.table("cdh_abfm_phi.visitdiagnosis") #2) diagnosis
visit_diagnosis_cleaned = load_visitdiagnosis_table().where(F.col("encounterdate").between(study_period_starts,study_period_ends)) # 2) diagnosis
pxProm = spark.table("cdh_abfm_phi.patientproblem") #3) diagnosis

pxNoteProb = spark.table("cdh_abfm_phi.patientnoteproblem") #1) notes
pxNote = spark.table("cdh_abfm_phi.patientnote") #2 notes
pxResObs = spark.table("cdh_abfm_phi.patientnoteresultobservation") #3) notes

patientData = load_patient_table()



# COMMAND ----------

display(patientData)

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe detail cdh_abfm_phi.visit

# COMMAND ----------

# MAGIC %md ## Cleaning functions

# COMMAND ----------

def cleanDate(date):
    return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")

# COMMAND ----------

#def cleanICD10(df,col_name):
#    newDf = df.withColumn(col_name, regexp_replace(col_name, '.', ''))


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


def find_partial_matches(df, words_to_match, columns_to_search):
    regex_pattern = "|".join(words_to_match)
    #conditions = [F.col(col_name).rlike(pattern) for col_name in df.columns for pattern in regex_pattern]
    conditions = [F.col("concat_col_temp").rlike(pattern) for pattern in regex_pattern]
    concat_col = (
        df.select("patientuid", "clean_date", *columns_to_search,
                  concat(*[ concat(F.lit(" "),(F.lower(F.col(column)))) for column in columns_to_search],F.lit(" ")).alias("concat_col_temp")
                  )
        .where(reduce ( lambda a , b: a | b, conditions))
        )
    return concat_col


# COMMAND ----------

# MAGIC %md # Visit, Visit diagnosis, patient problem

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_data_set_presentation_v2_run9

# COMMAND ----------

#display(visit)
#display(visit_diagnosis_cleaned)
#display(pxProm)

visit_sdf = (
    visit
    .select(
        "patientuid",
        F.col("encountertypecode").alias("diagnosis_code"),
        F.col("encounterstartdate").alias("diagnosis_date"),
        "practiceid"
        )
)

visit_diagnosis_cleaned_sdf = (
    visit_diagnosis_cleaned
    .select(
        "patientuid",
        F.col("encounterdiagnosiscode").alias("diagnosis_code"),
        F.col("encounterdate").alias("diagnosis_date"),
        "practiceid"
    )
)

pxProm_sdf = (
    pxProm
    .select(
        "patientuid",
        F.col("problemcode").alias("diagnosis_code"),
        F.col("documentationdate").alias("diagnosis_date"),
        "practiceid"
    )
)

diagnosis_table = (
    visit_sdf
    .unionByName(visit_diagnosis_cleaned_sdf)
    .unionByName(pxProm_sdf)    
)

diagnosis_table_clean = (
    diagnosis_table
    .withColumn("diagnosis_code", F.regexp_replace("diagnosis_code",'\.',''))
    .withColumn("diagnosis_date",cleanDate("diagnosis_date"))
)

suceptible_infected_sdf = (
    diagnosis_table_clean
    .where(        
        (F.col("diagnosis_code").isin(codes)) & (F.col("diagnosis_date").between(study_period_starts,study_period_ends))
    )
    .withColumn(
        "exposed",
        F.when(F.col("diagnosis_code").isin(mpox_codes),1)
        .otherwise(0)
        )
    .join(patientData.select("patientuid","age_group","gender"),["patientuid"], 'left')     
)
display(suceptible_infected_sdf)

# COMMAND ----------

# None of the exposed = 1 is missing dempgraphic information
#display(suceptible_infected_sdf.where("age_group is null"))
#display(suceptible_infected_sdf.where("gender is null"))

# COMMAND ----------

# finding who was ever exposed and getting their index date, for this specific condition the data scientist should verify if patient gets treated in multiple (distinct) practices
# in this case they just go to one practice

ever_exposed = (
    suceptible_infected_sdf
    .where("exposed == 1 ")
    .orderBy("patientuid", "diagnosis_date")
    .groupBy("patientuid","practiceid","age_group", "gender")
    .agg(                
        (F.collect_list(F.col("diagnosis_date")).getItem(0)).alias("diagnosis_date")
    ) 
    .withColumn("diagnosis_date_e", F.date_format("diagnosis_date","yyyy-MM")) 
    .withColumn("exposed", F.lit(1))
)

display(ever_exposed)

ever_exposed.drop("diagnosis_date_e")

# COMMAND ----------

non_exposed = (
    suceptible_infected_sdf
    .where("exposed == 0 ")
    .orderBy("patientuid", "diagnosis_date")
    .withColumn(
        "rand_rn",
        F.row_number().over(Window.partitionBy("patientuid")          
                            .orderBy("diagnosis_date")
                            .orderBy(F.rand(seed=42))
        ),
    )
    .filter("rand_rn == 1") # selectring random encounter that would be used for matching
    .withColumn(
        "diagnosis_date_ne", # diagnosis month for the non-exposed (but suceptible)
        F.date_format(F.to_date(F.col("diagnosis_date"),'yyyy-MM-dd'),"yyyy-MM")
    )
    .drop("rand_rn")
    .drop("diagnosis_code")
)

display(non_exposed)
non_exposed.cache()

# COMMAND ----------

display(ever_exposed)

# COMMAND ----------

exposed_type = (
    ever_exposed
        .select(
            F.col("patientuid").alias("case_id"),
            "age_group",          
            "gender",
            "diagnosis_date_e",             
            F.row_number().over(
                Window.partitionBy("age_group", "gender","diagnosis_date_e").orderBy("patientuid")
            ).alias("case_rn")
        )
    .withColumnRenamed("age_group", "age_group_case")
    .withColumnRenamed("gender", "gender_case")
)



# COMMAND ----------

# Assigns a unique value to assigned_person_number for every id in controls that the same age and gender, since we are matchig by exposure month and the month in which the pregnancy started, we know the "unexposed" are align with the exposed cases 
unexposed_person_number = (    
    non_exposed    
        .select(
            F.col("patientuid").alias("person_id"),
            "age_group",          
            "gender",
            "diagnosis_date_ne",  
            F.row_number().over(                
                Window.partitionBy("age_group", "gender","diagnosis_date_ne").orderBy(F.expr("uuid()"))
            ).alias("assigned_person_number")
        )
    .withColumnRenamed("age_group", "age_group_control")
    .withColumnRenamed("gender", "gender_control")    
)

# COMMAND ----------

CASE_CONTROL_RATIO = 2 # 2:1

# for each covariate (age, gender, region), it assigns the first CASE_CONTROL_RATIO persons to the first case_rn
# the second CASE_CONTROL_RATIO to the second case_rn, etc.
# Since for each covariate each person gets a unique assigned_person_number, no person can end up in two groups.

exposed_unexposed_out = (
    exposed_type
        .join(
            unexposed_person_number,
            on=(
                (unexposed_person_number.age_group_control == exposed_type.age_group_case) &
                (unexposed_person_number.gender_control == exposed_type.gender_case) &
                (unexposed_person_number.diagnosis_date_ne   == exposed_type.diagnosis_date_e)  &                                          
                # mathcing on the mpox, gender and touch point for controls on the same month
                             
                (
                    unexposed_person_number.assigned_person_number
                    .between(CASE_CONTROL_RATIO*(F.col("case_rn") - 1)+1, CASE_CONTROL_RATIO*F.col("case_rn"))
                )
            ),
            how="inner"
        )
        .orderBy("case_id", "person_id")
)

eu_checker = (
    exposed_unexposed_out.groupBy("case_id")
).agg(
    F.max(F.col("assigned_person_number")).alias("number_of_exposed")    
).agg(
    F.min(F.col("number_of_exposed")).alias("max_min_num_exposed")
)


# COMMAND ----------

display(exposed_unexposed_out)
display(eu_checker)

# COMMAND ----------

hvid_to_remove_exposed_unexposed = (
    exposed_unexposed_out
    .groupBy("case_id")
    .agg(
        F.collect_list("person_id").alias("n_unexposed"),        
    )
    .withColumn("array_size",
             F.size("n_unexposed")
               )
    .filter(F.col("array_size") < CASE_CONTROL_RATIO)        
)

exposed_remove = hvid_to_remove_exposed_unexposed.select(F.col("case_id").alias("patientuid"))
control_remove = hvid_to_remove_exposed_unexposed.select(F.col("n_unexposed").getItem(0).alias("patientuid"))
hvid_remove = exposed_remove.unionByName(control_remove)

# if this query returns empy then we had enough controls assigned to the cases
display(hvid_remove)

# COMMAND ----------

# MAGIC %md
# MAGIC Up to this point we have the ids of the cases and the controls. We need to:
# MAGIC - Match back these id to get the exposure dates
# MAGIC - Select the relevant note using a threshold for a look back period that we will use to bring the notes

# COMMAND ----------

# retrieving the information from the controls that were matched, we do not need to do this for the cases.

non_exposed_matched = (
    exposed_unexposed_out.select("person_id","diagnosis_date_ne")
    .join(
        non_exposed.alias("ne"), 
        (exposed_unexposed_out.person_id == F.col("ne.patientuid")) & (exposed_unexposed_out.diagnosis_date_ne ==  F.col("ne.diagnosis_date_ne"))        
    )
    .drop("diagnosis_date_ne")
    .drop(exposed_unexposed_out.person_id )
)

display(non_exposed_matched)
non_exposed_matched.count()


# COMMAND ----------

all_cohort = (
    non_exposed_matched.drop("diagnosis_date_ne")
    .unionByName(ever_exposed.drop("diagnosis_date_e"))    
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding the notes
# MAGIC
# MAGIC Compile notes, then select notes between -1 and 30 of the diagnosis_date for both groups. That would be the cohort 

# COMMAND ----------

pxNoteProbFiltered = (
    pxNoteProb
    .select(
        "*",
        (cleanDate("encounterdate")).alias("note_date"),                          
    ).select("patientuid", "note_date", F.lower(F.col("note")).alias("target_col"))
       
)

pxNote_cleanded = (
    pxNote.select("*",
                  (concat_ws(" ", 
                             F.col("sectionname"),
                             F.col("note"), 
                             F.col("group1"), 
                             F.col("group2"), 
                             F.col("group3"), 
                             )).alias("target_col_temp"),
                  (cleanDate(F.col("encounterdate"))).alias("note_date")
                  
                  )
    .withColumn("target_col", F.lower("target_col_temp"))
    .where(
        (F.col("note_date") >= study_period_starts)
        )          
    .select("patientuid", "note_date", "target_col")
)

pxResObsFilter = ( #2 amd 3
    pxResObs
    .select(
        "patientuid",
        cleanDate(F.col("encounterdate")).alias("note_date"),
        (F.lower(
            concat_ws(" ", 
                             F.col("sectionname"),
                             F.col("note"),                              
                             F.col("group2"), 
                             F.col("group3"), 
                             )
        )).alias("target_col")
    )
    .where(
        (F.col("note_date") >= study_period_starts)
        )
)

patient_notes = (
    pxNoteProbFiltered
    .unionByName(pxNote_cleanded)
    .unionByName(pxResObsFilter)
    .dropDuplicates()
    .withColumnRenamed("target_col", "notes")
    .where(
        (F.col("note_date") >= study_period_starts)
        )
    .sort("patientuid","note_date")
    .dropDuplicates()
)


display(patient_notes)
#patient_notes.count()

# COMMAND ----------

all_cohort_notes = (
    all_cohort
    .join(patient_notes,         
          all_cohort.patientuid ==  patient_notes.patientuid
          , 'left'          
          )
    .drop(patient_notes.patientuid) 
    .withColumn("date_diff_diagnosis_note",                
                F.datediff(F.col("diagnosis_date"),F.col("note_date"))
                )
    .where(F.col("date_diff_diagnosis_note").between(1,30)) # one month before diagnosis, we are looking these notes for additional symtoms    
)
display(all_cohort_notes)
all_cohort_notes.count()

# COMMAND ----------

DB_EXPLORATORY = 'cdh_abfm_phi_exploratory'
DB_NAME= 'cohort_notes'
USER = 'run9'


(
    all_cohort_notes
    .write    
    .mode('overwrite')
    .saveAsTable(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")
)


# COMMAND ----------



# COMMAND ----------


