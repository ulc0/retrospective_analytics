# Databricks notebook source
#import pandas as pd

from datetime import date
from pyspark.sql import SparkSession, DataFrame
#from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from functools import reduce

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

# MAGIC %run /CDH/Analytics/ABFM_PHI/Projects/cdh-abfm-phi-core/YYYY-MM-DD_abfm_template_notebooks/includes/0000-utils-high

# COMMAND ----------

# MAGIC %md
# MAGIC #Featurization

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

codes = mpox_codes+STI_visit_codes+NYC_STI



# COMMAND ----------

# MAGIC %md
# MAGIC # Cohort

# COMMAND ----------

# MAGIC %md
# MAGIC There should be NO NOTES TABLES referenced in this code

# COMMAND ----------

#visitDiag = spark.table("edav_prd_cdh.cdh_abfm_phi.visitdiagnosis")

#pxSocHis = spark.table("edav_prd_cdh.cdh_abfm_phi.patientsocialhistoryobservation")
pxResultObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientresultobservation")
pxNoteResultObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation")

#pxProcedure =spark.table("edav_prd_cdh.cdh_abfm_phi.patientprocedure")
pxProm = spark.table("edav_prd_cdh.cdh_abfm_phi.patientproblem")
#pxNoteProb = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteproblem")

#pxNote = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnote")
pxImunizaion = spark.table("edav_prd_cdh.cdh_abfm_phi.patientimmunization")

patientData = load_patient_table()


######################################################
######################################################
# need to put the 'note' tables together and the pxproblem tables together with a left join so we have diagnosis, dates and patient notes
######################################################
######################################################

# COMMAND ----------

# MAGIC %md
# MAGIC pxNote_filtered = (
# MAGIC     pxNote
# MAGIC     .select(
# MAGIC         "patientuid",        
# MAGIC         (cleanDate(F.col("encounterdate"))).alias("note_date"),
# MAGIC          F.lower(deleteMarkup((concat_ws(" ", "note")))).alias("target_col")
# MAGIC     )
# MAGIC     .where(
# MAGIC         (F.col("note_date") >= study_period_starts)
# MAGIC         )
# MAGIC )
# MAGIC
# MAGIC display(pxNote_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC display(pxNote
# MAGIC         .where("""                        
# MAGIC
# MAGIC                lower(sectionname) like "%monkeypox%"
# MAGIC             or lower(sectionname) like "%monkypox%"
# MAGIC             or lower(sectionname) like "%monkey pox%"
# MAGIC             or lower(sectionname) like "%monkepox pox%"
# MAGIC             or lower(sectionname) like "%monky pox%"
# MAGIC             or lower(sectionname) like "%monkey-pox%"
# MAGIC             or lower(sectionname) like "%monke-pox%"
# MAGIC             or lower(sectionname) like "%monky-pox%"
# MAGIC             or lower(sectionname) like "%orthopox%"
# MAGIC             or lower(sectionname) like "%orthopoxvirus%"
# MAGIC             or lower(sectionname) like "%parapoxvirus%"           
# MAGIC             or lower(sectionname) like "%monk%"           
# MAGIC             or lower(sectionname) like "%ortho pox%"
# MAGIC             or lower(sectionname) like "%poxvi%"      
# MAGIC             or lower(sectionname) like "%monkeypox%" 
# MAGIC             or lower(sectionname) like "%mpox%" 
# MAGIC             or lower(sectionname) like "%m-pox%"
# MAGIC             or lower(sectionname) like "%mpx%"
# MAGIC
# MAGIC             or lower(note) like "%monkeypox%"
# MAGIC             or lower(note) like "%monkypox%"
# MAGIC             or lower(note) like "%monkey pox%"
# MAGIC             or lower(note) like "%monkepox pox%"
# MAGIC             or lower(note) like "%monky pox%"
# MAGIC             or lower(note) like "%monkey-pox%"
# MAGIC             or lower(note) like "%monke-pox%"
# MAGIC             or lower(note) like "%monky-pox%"
# MAGIC             or lower(note) like "%orthopox%"
# MAGIC             or lower(note) like "%orthopoxvirus%"
# MAGIC             or lower(note) like "%parapoxvirus%"           
# MAGIC             or lower(note) like "%monk%"           
# MAGIC             or lower(note) like "%ortho pox%"
# MAGIC             or lower(note) like "%poxvi%"      
# MAGIC             or lower(note) like "%monkeypox%" 
# MAGIC             or lower(note) like "%mpox%" 
# MAGIC             or lower(note) like "%m-pox%"
# MAGIC             or lower(note) like "%mpx%" 
# MAGIC            
# MAGIC            """).sort("encounterdate", ascending = False)
# MAGIC        )

# COMMAND ----------

# MAGIC %md #patientnoteresultobservation 

# COMMAND ----------

# MAGIC %md
# MAGIC pxResObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation")

# COMMAND ----------

# MAGIC %md
# MAGIC pxResObsFilter = (
# MAGIC     pxResObs
# MAGIC     .select(
# MAGIC         "patientuid",
# MAGIC         cleanDate(F.col("encounterdate")).alias("note_date"),
# MAGIC         F.lower(deleteMarkup("note")).alias("target_col")
# MAGIC     )
# MAGIC     .where(            
# MAGIC             #(F.col("target_col").rlike("|".join(words_to_match_mpox)) & 
# MAGIC              (F.col("note_date") >= study_period_starts)
# MAGIC              )
# MAGIC             #)
# MAGIC     #)
# MAGIC )
# MAGIC
# MAGIC display(pxResObsFilter)

# COMMAND ----------

# MAGIC %md
# MAGIC patient_notes = (
# MAGIC     pxNote_filtered.unionByName(pxResObsFilter)
# MAGIC     .dropDuplicates()
# MAGIC     .withColumnRenamed("target_col", "notes")
# MAGIC     #.where(F.length(F.col("notes")) >= 50)
# MAGIC )
# MAGIC patient_notes.cache()
# MAGIC display(patient_notes)

# COMMAND ----------

# MAGIC %md
# MAGIC pxResObs = spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_textdoc")

# COMMAND ----------

targetPopulation.printSchema()
patient_notes.printSchema()

# COMMAND ----------

case_cohort_notes = (
    case_cohort_matched.join(patient_notes, ['patientuid'], 'left')
    .where(
        (abs(F.datediff(case_cohort_matched.mpox_dates, patient_notes.note_date))<=30) 
        #& 
        #(F.length(F.col("notes")) >= 50)
        )
)

display(case_cohort_notes)

# COMMAND ----------

case_cohort_notes.select("patientuid").dropDuplicates().count() # 22 patients with mpox do not have notes within 30 days before of after diagnosis...

# there are 185 patients with a mpox diagnosis or pox live virus (555 including the controls)

# out of those 163 have a patient note of more than 50 words, 


# COMMAND ----------

DB_EXPLORATORY = 'edav_prd_cdh.cdh_abfm_phi_exploratory'
DB_NAME= 'mpox_data_set'
USER = 'run9'

(
    case_cohort_notes
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")
)


# COMMAND ----------

print(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")

# COMMAND ----------

collected_notes = spark.read.table("edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_data_set_run9")

display(collected_notes)

# COMMAND ----------

# DBTITLE 1,a
s = list(set(collected_notes.columns) - {'notes'}-{'note_date'}) 

collected_notes_2 = (
    collected_notes
    .sort("note_date")
    .groupBy(s)
    .agg(
        F.collect_list("note_date").alias("note_dates"),
        F.collect_list("notes").alias("notes"),
    )
)

display(collected_notes_2)

# COMMAND ----------

display(collected_notes_2.select("patientuid", "exposed", "STI_encounter","age_group", "mpox_dates", "gender","statecode", "notes"))

# COMMAND ----------

collected_notes_2.select("patientuid").where("exposed = 1").count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_notes_txt sort by patient_id

# COMMAND ----------

# MAGIC %md ##  Testing splitted tables 

# COMMAND ----------

root = 'edav_prd_cdh.cdh_abfm_phi_exploratory'


listTables = [
    'ml_patientnote',
    'ml_patientnote_htm',
    'ml_patientnote_mpox',
    'ml_patientnote_rtf',
    'ml_patientnote_utf',
    'ml_patientnote_xml'
]



# COMMAND ----------

spark.table(f'{root}.ml_patientnote_mpox').where("mpox_notes= True").count()

# COMMAND ----------

print('ml_patientnote')
spark.table(f'{root}.ml_patientnote').display()
print('ml_patientnote_htm')
spark.table(f'{root}.ml_patientnote_htm').display()
print('ml_patientnote_mpox')
spark.table(f'{root}.ml_patientnote_mpox').where("mpox_notes= True").display()
# Provider id 1814 seems to have an html format but the spliting function does not take it because it starts with plain text, think of using some other way to identify and classify such as <div> or </div> or <div (something)>

# test person_id 7747D1FB-04AA-48F4-A6A7-E239DE03211C for uniqueness of note, two records have the same note 

#411

print('ml_patientnote_rtf')
spark.table(f'{root}.ml_patientnote_rtf').display()
print('ml_patientnote_utf')
spark.table(f'{root}.ml_patientnote_utf').display()
print('ml_patientnote_xml')
spark.table(f'{root}.ml_patientnote_xml').display()


# COMMAND ----------

# MAGIC %md # Testing Pretrained models

# COMMAND ----------

# MAGIC %md ## Biomedical-ner-all

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")

pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple") # pass device=0 if using gpu



# COMMAND ----------

out = pipe("""["Patient here for annual wellness visit. Patient last visit july 25, 2022. Patient blood test may 2022. Patient home exercise no just walking 1 hour 2 x a week. Patient no exposure to HIV, no IV drug use no. Patient never smoked no. Patient denies alcohol intake no. Patient denies depression no. Patient last bone density dexa scan never. Patient has problem on bowel movement constipated worse past 6 months. patient failed on otc colace, metamucil, senokot. Patient last colonoscopy 2022 january colon polyp repeat in 3-5 years dr cremoninn. Patient last eye exam november 2021 no glaucoma. Patient flu shot october 2021. Patient pneumonia shot 2019 and 2020. Patient last prostate exam october 2021. patient covid x 2, 2021 booster september 2021 and march 2022. monkeypox vaccine august 5 #1. patient had covid + july 20, 2022. patient stopped metformin stopped metformin 6 months or sometimes with heavy meal."]""")
out

# COMMAND ----------

out = pipe(
"""ent recommendations from CDC, pre-exposure immunization is not recommended at this time for persons who do not have significant occupational exposure to monkeypox or monkeypox-infected individuals, even in the setting of significant - Pt inquires about vaccination to prevent monkeypox transmission.'""")

out

# COMMAND ----------

cleanedNotes_transformers_mpox = (
    spark.table(f'{root}.ml_patientnote_mpox').withColumn("note_datetime", cleanDate("note_datetime"))#.where("mpox_notes= True")
)


# COMMAND ----------

cleanedNotes_transformers_mpox.display()

# COMMAND ----------

selected_notes = (
    cleanedNotes_transformers_mpox.select("person_id","note_text","provider_id","mpox_notes", "note_datetime").dropDuplicates()
    .where("mpox_notes = true")
    .groupBy("person_id","note_datetime")
    .agg(
        F.collect_list("note_text").alias("daily_notes"),
        F.collect_list("provider_id").alias("providers"),
    )
)

display(selected_notes)

# COMMAND ----------

selected_notes_pdf = selected_notes.toPandas()
selected_notes_pdf

# COMMAND ----------

from Bio_Epidemiology_NER.bio_recognizer import ner_prediction

# COMMAND ----------

doc = selected_notes_pdf.iloc[7,2]

# COMMAND ----------

doc_string = ' '.join([str(elem) for elem in doc])
doc_string

# COMMAND ----------

type(doc_string)

# COMMAND ----------

temp_output = ner_prediction(corpus = doc_string, compute = 'cpu')
temp_output

# COMMAND ----------

# MAGIC %md ## Spacy

# COMMAND ----------

# MAGIC %pip install markupsafe==2.0.1

# COMMAND ----------

# MAGIC %pip install spacy

# COMMAND ----------

# MAGIC %pip install scispacy

# COMMAND ----------

# MAGIC %pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_scibert-0.5.1.tar.gz

# COMMAND ----------

# sample notes

note0 = ["""Note:_<p>Willica presents today for a follow-up. She is currently working at Henderson. Her EKG was normal. She notes that she has been feeling tired lately. She notes that she is cold a lot. She notes that her heart rate is slightly elevated, and at first it was 100 beats a minute, and then when she checked it again, it was around 90 bpm. She reports that she thought she had monkeypox. She notes that bumps popped up a couple of weeks ago on her hands, face, and foot, but they went away. She notes that she still has a period, and it is heavy the first day, but the other days are normal. She denies any chance of pregnancy since her husband is getting a vasectomy. She notes that there have been no changes with her period lately. She reports that her blood pressure is normal. She is currently taking Acetazolamide. She notes that she is getting an EGD tomorrow. She notes that she has a history of cedar tumors, which are controlled. She reports that her dose of Acetazolamide has not changed recently, and she has always been on 2 twice a day. Her lipids back in January were normal, and her sugar was normal. She is not fasting right now. She had a screening test for diabetes less than a year ago, and it came back normal. She notes that her mother stresses her out before bedtime. Her dad helps take the pressure off of her that her mom puts on her. She states that her mom is not acting better. She states that she has not had her flu shot. </p>"""] 

note1 = ["Doubt monkey pox, or zoster.|Hope is not mrsa|Cover with keflex and rto 3-4 d if not responding|9/16/22  Small infection is gone.  Lump itself is not tender now, but same size.  We will refer to surgery for their opinion"]

note2 = ["""RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs; CARDIOVASCULAR: normal rate; rhythm is regular; no systolic murmur; GASTROINTESTINAL: nontender; normal bowel sounds; LYMPHATICS: no adenopathy in cervical, supraclavicular, axillary, or inguinal regions; BREAST/INTEGUMENT: no rashes or lesions; MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone; NEUROLOGIC: appropriate for age; Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius"""]

sequences = note0 + note1 + note2

# COMMAND ----------

sequences_str = ' '.join(str(n) for n in sequences)

# COMMAND ----------

import scispacy
import spacy

from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_sci_scibert")
#text = """
#Myeloid derived suppressor cells (MDSC) are immature 
#myeloid cells with immunosuppressive activity. 
#They accumulate in tumor-bearing mice and humans 
#with different types of cancer, including hepatocellular 
#carcinoma (HCC).
#"""
doc = nlp(sequences_str)

print(list(doc.sents))

# COMMAND ----------

for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)

# COMMAND ----------

nlp.get_pipe("ner").labels

# COMMAND ----------

nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

doc = nlp("Spinal and bulbar muscular atrophy (SBMA) is an \
           inherited motor neuron disease caused by the expansion \
           of a polyglutamine tract within the androgen receptor (AR). \
           SBMA can be caused by this easily.")

doc


# COMMAND ----------

# MAGIC %md Testing the following task
# MAGIC
# MAGIC 1. Zero shot classification
# MAGIC 2. Name entity recognition
# MAGIC 3. Sentiment analysis
# MAGIC 4. Text classification
# MAGIC

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")
model = AutoModel.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")


# COMMAND ----------

# MAGIC %md ## Clinical bert 

# COMMAND ----------

# MAGIC %md ### Testing for alternative zero shot (facebook/bart-large-mnli)

# COMMAND ----------

# MAGIC %md ## GatorTron-0G

# COMMAND ----------

# MAGIC %md ## PHS-BERT

# COMMAND ----------

# MAGIC %md ## DIstilled BERT

# COMMAND ----------

# MAGIC %md # Additional elements

# COMMAND ----------

# MAGIC %md ## Large tables for Kate - mpox

# COMMAND ----------

display(pxNote)

pxNote_large_mpox = (
    pxNote.select("patientuid","encounterdate","note")
    .where((F.col("note").rlike("|".join(words_to_match_mpox)))
    )

    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

display(pxNote_large_mpox)
pxNote_large_mpox.count()

# COMMAND ----------

display(pxResObs)

pxResObs_large_mpox = (
    pxResObs.select("patientuid","encounterdate","note")
    .where((F.col("note").rlike("|".join(words_to_match_mpox)))
    )

    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)
#display(pxResObs_large_mpox)
#pxResObs_large_mpox.count()


# COMMAND ----------

mpox_notes = pxResObs_large_mpox.unionByName(pxNote_large_mpox)
db = "edav_prd_cdh.cdh_abfm_phi_exploratory"
version_run1 = "mpox_large"

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory_run9_{}""".format(version_run1))
mpox_notes.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run1}")

# COMMAND ----------

spark.table(f"{db}.run9_{version_run1}").display()
spark.table(f"{db}.run9_{version_run1}").count()

# COMMAND ----------

# MAGIC %md ## All text table

# COMMAND ----------

pxNote_large = (
    pxNote.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

pxNote_large = (
    pxResObs.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)
#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

notes = pxNote_large.unionByName(pxNote_large)
db = "edav_prd_cdh.cdh_abfm_phi_exploratory"
version_run2 = "large"

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS edav_prd_cdh.cdh_abfm_phi_exploratory_run9_{}""".format(version_run2))
notes.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run2}")

# COMMAND ----------

spark.table(f"{db}.run9_{version_run2}").display()
spark.table(f"{db}.run9_{version_run2}").count()


# COMMAND ----------

f"{db}.run9_{version_run2}"
