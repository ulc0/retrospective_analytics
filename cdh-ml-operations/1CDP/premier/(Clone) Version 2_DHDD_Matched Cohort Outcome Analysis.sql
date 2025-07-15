-- Databricks notebook source
-- MAGIC %md
-- MAGIC #DHDD - Matched Cohort Outcome Analysis
-- MAGIC
-- MAGIC Name: Afua Nyame-Mireku
-- MAGIC
-- MAGIC Assigned: 5/2/2024

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Program Request
-- MAGIC
-- MAGIC Premier analysis of people who have Intellectual and/or Developmental disabilities (IDD) and/or Specific Learning and Developmental Disabilities (SLDD)
-- MAGIC
-- MAGIC The program is primarily interested in the ability of Premier (or other relevant data sources) to be used as a part of surveillance system to monitor the prevalence of IDD and the health and wellbeing of adults with IDD. They have included codes labeled as IDD and codes labeled Specific Learning and Developmental Disabilities (SLDD) to stay consistent with their definitions from their syndromic surveillance work but ideally, they’d like to look at the data a few different ways so they can compare to past projects and the literature.  
-- MAGIC
-- MAGIC - The goal is to compare outcomes among population with disability vs population without disability, matching for age, sex, year of admission, and quarter of admission:
-- MAGIC   - Create 6 study populations to compare people who have the disability to people who do not have the disability
-- MAGIC     - IDD vs. No IDD
-- MAGIC     - SLDD vs. No SLDD
-- MAGIC     - IDD/SLDD vs. No IDD/SLDD 
-- MAGIC   - Stratify the outcomes by age within each study population: 18-64, 65+ 
-- MAGIC   - Inclusion criteria: all inpatients 18 or older between 2016 - 2023
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Relevant Databases, Tables, and Variables
-- MAGIC
-- MAGIC The databases, patient tables, lookup tables, and variables needed for this analysis are listed below.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Database: cdh_premier_v2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC #### Patient Table: patdemo
-- MAGIC
-- MAGIC **pat_key:** Unique hospital encounter identifier (de-identified)
-- MAGIC
-- MAGIC **medrec_key:** Unique patient identifier (de-identified), defined by medical record number and hospital entity. Use this field to track a patient across multiple inpatient and/or outpatient encounters at the same hospital.
-- MAGIC
-- MAGIC **age:** Patient age in years, calculated as admission date - date of birth. Patient age 90 and above are assigned an age of 89 per HIPAA regulations
-- MAGIC
-- MAGIC **admit_date**: Admission Date
-- MAGIC
-- MAGIC **discharge_date**: Discharge Date
-- MAGIC
-- MAGIC **disc_mon:** Month and year patient was discharged, formatted: YYYYQMM 
-- MAGIC
-- MAGIC **disc_status:** UB-04 Discharge status code
-- MAGIC
-- MAGIC **los:** Hospital submitted length of stay. Applies to inpatient encounters only.
-- MAGIC
-- MAGIC **gender:** UB-04 Gender designation
-- MAGIC
-- MAGIC **hispanic_ind:** Hispanic indicator (derived from the UB-04 Ethnicity designation)
-- MAGIC
-- MAGIC **i_o_ind:** Inpatient/Outpatient indicator
-- MAGIC
-- MAGIC **los:** Hospital submitted length of stay. Applies to inpatient encounters only.
-- MAGIC
-- MAGIC **pat_type:** A Premier mapped field to denote service type of hospital encounter. All hospitals must submit Inpatient (08) encounters to Premier.
-- MAGIC
-- MAGIC **race:** UB-04 Race designation. Some race designations have been rolled into "Other" to ensure that the data set conforms to HIPAA and other regulatory requirements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Patient Table: paticd_diag
-- MAGIC **icd_code:** ICD-9 or ICD-10 diagnosis code. Use the icd_version field to differentiate to which ICD code set the ICD code belongs, as there are some ICD codes that overlap between ICD-9 and ICD-10 code sets.
-- MAGIC
-- MAGIC **icd_pri_sec:** Indicates whether an ICD diagnosis code is: Admitting, Principal or Secondary
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Lookup Table: disstat
-- MAGIC **disc_status_desc:** UB-04 Discharge status description

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Patient Table: patbill
-- MAGIC **std_chg_code:** Premier Standard Charge Master code
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Lookup Table: Chgmstr
-- MAGIC
-- MAGIC **std_chg_desc:** Premier Standard Charge Master description
-- MAGIC
-- MAGIC **clin_dtl_code:** Premier Clinical Detail code
-- MAGIC
-- MAGIC **clin_sum_code:** Premier Clinical Summary code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Database: cdh_reference_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Table: ccsr_codelist
-- MAGIC **CCSR_category**
-- MAGIC
-- MAGIC **CCSR_category_desc**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Updated IDD/SLDD definitions
-- MAGIC Exclude inpatients from IDD/SLDD diagnosis if in the following criteria:
-- MAGIC - G70.00, G70.01, G70.1, G70.2, G70.80, G70.81, G70.89, G70.9	Lambert-Eaton Syndrome	Group E) Other disability				
-- MAGIC - F63.0	Pathological gambling	
-- MAGIC - F63.1	Pyromania
-- MAGIC - F63.2	Kleptomania	
-- MAGIC - F63.3	Trichotillomania	
-- MAGIC - F63.81	Intermittent explosive disorder	
-- MAGIC - F63.89	Other impulse disorders	
-- MAGIC - F63.9	Impulse disorder, unspecified	
-- MAGIC - R47.0, R47.01, R47.02, R47.1, R47.8, R47.81, R47.82, R47.89, R47.9	Speech disturbances, not elsewhere classified
-- MAGIC - R48.1	Agnosia	
-- MAGIC - R48.2	Apraxia	
-- MAGIC - R48.3	Visual agnosia	
-- MAGIC - R48.8	Other symbolic dysfunctions	
-- MAGIC - R48.9	Unspecified symbolic dysfunctions	
-- MAGIC 			
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Import Packages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from scipy.stats import chi2_contingency
-- MAGIC import pandas as pd
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Tables (Use Exploratory Tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Premier Lifetime Diagnosis of IDD and SLDD Dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Premier Lifetime Diagnosis of (IDD and SLDD)
-- MAGIC dx=spark.sql("""
-- MAGIC with alldx as (
-- MAGIC select distinct
-- MAGIC d.PAT_KEY,
-- MAGIC case
-- MAGIC  when d.ICD_CODE  in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2','Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0',
-- MAGIC                       'Q92.1','Q92.2','Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2','Q93.3','Q93.4','Q93.5','Q93.51',
-- MAGIC                       'Q93.59','Q93.7','Q93.81','Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2') then 1
-- MAGIC  else 0
-- MAGIC end as IDDA,
-- MAGIC
-- MAGIC case 
-- MAGIC    when d.ICD_CODE in ('G80.0','G80.1','G80.2','G80.3','G80.4','G80.8','G80.9') then 1
-- MAGIC    else 0
-- MAGIC end as IDDB,
-- MAGIC
-- MAGIC case 
-- MAGIC   when d.ICD_CODE in ('F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9') then 1
-- MAGIC   else 0
-- MAGIC end as IDDC,
-- MAGIC
-- MAGIC
-- MAGIC case 
-- MAGIC  when d.ICD_CODE in ('Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79') then 1 else 0 
-- MAGIC end as IDDD,
-- MAGIC
-- MAGIC case
-- MAGIC  when d.ICD_CODE in ('F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 'Q04.3', 'Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3',
-- MAGIC                      'Q67.6', 'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03', 'Q70.10', 'Q70.11', 'Q70.12', 
-- MAGIC                      'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9',
-- MAGIC                      'Q85.1','Q87.1','Q87.19','Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83') then 1 else 0
-- MAGIC end as IDDE,
-- MAGIC
-- MAGIC case
-- MAGIC  when d.ICD_CODE in ('F80.0','F80.1','F80.2','F80.4','F80.81','F80.82','F80.89',
-- MAGIC                      'F80.9','F81.0','F81.2','F81.81','F81.89','F81.9','F82','F90.0','F90.1','F90.2','F90.8','F90.9','F91.0','F91.1',
-- MAGIC                      'F91.2','F91.3','F91.8','F91.9','H93.25',
-- MAGIC                      'R48.0') then 1 else 0 
-- MAGIC end as SLDD,
-- MAGIC d.year
-- MAGIC from (select pat_key, icd_code, year from cdh_premier_v2.paticd_diag) d
-- MAGIC )
-- MAGIC
-- MAGIC select distinct pat_key,year,IDDA,IDDB,IDDC,IDDD,IDDE,greatest(IDDA,IDDB,IDDC,IDDD,IDDE) as IDD,SLDD from alldx 
-- MAGIC where 1=1 
-- MAGIC and (IDDA=1 or IDDB=1 or IDDC=1 or  IDDD=1 or  IDDE=1 or SLDD=1)
-- MAGIC order by PAT_KEY
-- MAGIC """)
-- MAGIC dx.createOrReplaceTempView('PremierIDDSLDD2')

-- COMMAND ----------

select concat ('idd = 1: ', count(*))
from premieriddsldd2
where idd = 1
union 
select concat ('sldd = 1: ', count(*))
from premieriddsldd2
where sldd = 1
union
select concat ('idd = 1 or sldd = 1: ', count(*))
from premieriddsldd2
where idd = 1 or sldd = 1
union
select concat ('idd = 1 and sldd = 1: ', count(*))
from premieriddsldd2
where idd = 1 and sldd = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###All Inpatients Dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Inpatients (All inpatients in Premier dataset i.e. all years)
-- MAGIC inp=spark.sql("""
-- MAGIC    select distinct
-- MAGIC     d.medrec_key,
-- MAGIC     d.pat_key,
-- MAGIC     d.Admit_date,
-- MAGIC     d.year,
-- MAGIC     d.month,
-- MAGIC     d.age,
-- MAGIC     d.race,
-- MAGIC     d.hispanic_ind,
-- MAGIC     d.gender,
-- MAGIC     case
-- MAGIC       when d.age <1 then 'a<1'
-- MAGIC       when d.age between 1 and 5 then 'b1-5'
-- MAGIC       when d.age between 6 and 19 then 'c6-19' 
-- MAGIC       when d.age between 20 and 64 then 'd20-64'
-- MAGIC       else  'e65+' 
-- MAGIC     end as agegrp,
-- MAGIC     case 
-- MAGIC      when d.age between 18 and 64 then 'a18-64'
-- MAGIC      when d.age >=65 then 'b65+'
-- MAGIC      else 'cunknown'
-- MAGIC     end as agegrp2,
-- MAGIC    case 
-- MAGIC        when d.std_payor in (360,370,380) then 'cPrivate'
-- MAGIC        when d.STD_PAYOR in (300,310,320) then 'bMedicare'
-- MAGIC        when d.std_payor in (330,340,350) then 'aMedicaid'
-- MAGIC        when d.std_payor in (410) then 'dSelf_pay'
-- MAGIC        --else 'NA'
-- MAGIC     end as ins,
-- MAGIC     datediff(d.Discharge_date,d.Admit_date) as losc,
-- MAGIC     d.los,
-- MAGIC     d.disc_status,
-- MAGIC     dis.disc_status_desc,
-- MAGIC     a.std_chg_code, 
-- MAGIC     b.std_chg_desc, 
-- MAGIC     b.clin_dtl_code, 
-- MAGIC     b.clin_sum_code,
-- MAGIC     case when b.clin_sum_code =110102 then 1 else 0  end as ICU,
-- MAGIC     case when b.clin_dtl_code =410412946570007 then 1 else 0 end as Vent
-- MAGIC
-- MAGIC      from (select medrec_key,pat_key, pat_type, I_O_IND,age,std_payor,year(Admit_date) as year, month(Admit_date) as month,Admit_date,
-- MAGIC            Discharge_date,los,race,hispanic_ind,gender,disc_status 
-- MAGIC            from cdh_premier_v2.patdemo) d 
-- MAGIC       left join cdh_premier_v2.disstat dis on dis.disc_status=d.disc_status
-- MAGIC       left join (select pat_key, std_chg_code from cdh_premier_v2.patbill) a on a.pat_key = d.pat_key
-- MAGIC       left join (select std_chg_code, std_chg_desc, clin_dtl_code, clin_sum_code from cdh_premier_v2.chgmstr) b
-- MAGIC                   on a.std_chg_code = b.std_chg_code
-- MAGIC       where 1=1
-- MAGIC       and d.PAT_TYPE=8  --inpatient indicator (use both)
-- MAGIC       and d.I_O_IND='I' -- inpatient indicator (use both)
-- MAGIC       
-- MAGIC       order by d.pat_key
-- MAGIC """)
-- MAGIC inp.createOrReplaceTempView('PremierInp2')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IDD/SLDD Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### All patients with IDD/SLDD 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Premier Patients with disability - IDD/SLDD
-- MAGIC a=spark.sql("""
-- MAGIC            select distinct p.medrec_key,d.*
-- MAGIC            from PremierIDDSLDD2 d
-- MAGIC            left join (select distinct medrec_key,pat_key from cdh_premier_v2.patdemo) p  
-- MAGIC            on p.pat_key=d.pat_key
-- MAGIC                     where 1=1
-- MAGIC                     and (d.idd=1 or d.sldd=1)
-- MAGIC                     order by p.medrec_key   
-- MAGIC """)
-- MAGIC a.createOrReplaceTempView('PremierDisab')

-- COMMAND ----------

select concat ('idd = 1 or sldd = 1: ', count(*))
from premierdisab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Inpatients 18+ with IDD/SLDD

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2016-2023 Cohort of 18+ inpatients having disability  -IDD/SLDD                     
-- MAGIC b=spark.sql("""
-- MAGIC             select distinct medrec_key,pat_key,disc_status,disc_status_desc, los,max(age) over(partition by medrec_key) as agem, race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                  from PremierInp2 i
-- MAGIC                  where exists (select 1 from PremierDisab d
-- MAGIC                                where i.pat_key=d.pat_key
-- MAGIC                                and i.year between '2016' and '2023'
-- MAGIC                                and i.age>=18
-- MAGIC                                )
-- MAGIC                 order by medrec_key
-- MAGIC """)
-- MAGIC b.createOrReplaceTempView('c201623')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623")
-- MAGIC

-- COMMAND ----------

select concat ('idd = 1 or sldd = 1: ', count(distinct medrec_key))
from c201623

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inpatients 18+ without IDD/SLDD

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2016-2023 Cohort of 18+ inpatients having NO disability
-- MAGIC c=spark.sql("""
-- MAGIC              select distinct medrec_key,pat_key,disc_status, disc_status_desc, los, max(age) over(partition by medrec_key) as agem, race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                       from PremierInp2 i
-- MAGIC                       where 1=1
-- MAGIC                       and i.year between '2016' and '2023'
-- MAGIC                       and i.age>=18
-- MAGIC                       and not exists(select 1 from PremierDisab d
-- MAGIC                                      where i.pat_key=d.pat_key
-- MAGIC                                     )
-- MAGIC                       order by medrec_key
-- MAGIC """)
-- MAGIC c.createOrReplaceTempView('c201623_ND')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623_ND")

-- COMMAND ----------

select concat ('idd = 1 or sldd = 1: ', count(*))
from c201623_ND

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Matching (Use for Analysis)
-- MAGIC IDD/SLDD group & non IDD/SLDD group
-- MAGIC
-- MAGIC --Cohort A: Identify 2016 - 2023 inpatients who have IDD/SLDD (medrec_key, age, gender, year of admission, quarter of admission) and assign distinct cases a case number
-- MAGIC
-- MAGIC --Cohort B: Identify inpatients from 2016 - 2023 who did not have IDD/SLDD (medrec_key, age, gender, year of admission, quarter of admission)
-- MAGIC
-- MAGIC --Cross join disability and non-disability group 
-- MAGIC
-- MAGIC --Randomly select distinct control for each case to remove control duplicates
-- MAGIC
-- MAGIC --Isolate medrec_key for cases and add to relevant IDD, SLDD, or IDD/SLDD Table
-- MAGIC
-- MAGIC --Isolate c_medrec_key for controls and add to relevant IDD, SLDD, or IDD/SLDD Table
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched IDD/SLDD Dataset

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC #Matched IDD/SLDD  Dataset
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for controls. Include maximum age and gender for matching
-- MAGIC controls as (
-- MAGIC   select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC
-- MAGIC --Select distinct case ids from matched participants
-- MAGIC case_ids as (select distinct medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC --Limit original dataset to matched participants
-- MAGIC select * 
-- MAGIC from c201623
-- MAGIC where medrec_key in (select medrec_key from case_ids)
-- MAGIC
-- MAGIC """)
-- MAGIC r.createOrReplaceTempView('m_c201623')
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623")

-- COMMAND ----------

select concat ('idd = 1 or sldd = 1: ', count(distinct medrec_key))
from m_c201623

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched No IDD/SLDD Dataset

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC controls as (
-- MAGIC   select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC control_ids as (select distinct c_medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND
-- MAGIC where medrec_key in (select c_medrec_key from control_ids)
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC r.createOrReplaceTempView('m_c201623_ND')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623_ND
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_ND")

-- COMMAND ----------

select concat ('idd = 0 and sldd = 0: ', count(distinct medrec_key))
from m_c201623_ND



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IDD Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### All patients with IDD

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC a=spark.sql("""
-- MAGIC            select distinct p.medrec_key,d.*
-- MAGIC            from PremierIDDSLDD2 d
-- MAGIC            left join (select distinct medrec_key,pat_key from cdh_premier_v2.patdemo) p  on p.pat_key=d.pat_key
-- MAGIC                     where 1=1
-- MAGIC                     and (d.idd=1)
-- MAGIC                     order by p.medrec_key   
-- MAGIC """)
-- MAGIC a.createOrReplaceTempView('PremierDisab_idd')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inpatients 18+ with IDD 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 2016-2023 Cohort of 18+,inpatients with disability and other primary diagnosis on same encounter as 
-- MAGIC # disability diagnosis -IDD                    
-- MAGIC b=spark.sql("""
-- MAGIC             select distinct medrec_key,pat_key,disc_status, disc_status_desc, los,max(age) over(partition by medrec_key) as agem,race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                  from PremierInp2 i
-- MAGIC                  where exists (select 1 from PremierDisab_idd d
-- MAGIC                                where i.pat_key=d.pat_key  --inpatients enc corresponds to enc of disability diagnosis
-- MAGIC                                and i.year between '2016' and '2023'
-- MAGIC                                and i.age>=18
-- MAGIC                                )
-- MAGIC                 order by medrec_key
-- MAGIC """)
-- MAGIC b.createOrReplaceTempView('c201623_idd')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_idd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623_idd")

-- COMMAND ----------

select concat ('idd = 1: ', count(distinct medrec_key))
from c201623_idd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inpatients 18+ without IDD

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2016-2023 Cohort of 18+,inpatients WITHOUT disability diagnosis -IDD 
-- MAGIC c=spark.sql("""
-- MAGIC              select distinct medrec_key,pat_key,disc_status,disc_status_desc,los, max(age) over(partition by medrec_key) as agem, race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                       from PremierInp2 i
-- MAGIC                       where 1=1
-- MAGIC                       and i.year between '2016' and '2023'
-- MAGIC                       and i.age>=18
-- MAGIC                       and not exists(select 1 from PremierDisab_idd d
-- MAGIC                                      where i.pat_key=d.pat_key
-- MAGIC                                     )
-- MAGIC                       order by medrec_key
-- MAGIC """)
-- MAGIC c.createOrReplaceTempView('c201623_ND_idd')

-- COMMAND ----------

select concat ('idd = 0: ', count(distinct medrec_key))
from c201623_ND_idd

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND_idd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623_ND_idd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Matching (Use for Analysis)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched IDD

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %python
-- MAGIC #Matched IDD  Dataset
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_idd
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for controls. Include maximum age and gender for matching
-- MAGIC controls as (
-- MAGIC   select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND_idd
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC --Select distinct case ids from matched participants
-- MAGIC case_ids as (select distinct medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC --Limit original dataset to matched participants
-- MAGIC select * 
-- MAGIC from c201623_idd
-- MAGIC where medrec_key in (select medrec_key from case_ids)
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC r.createOrReplaceTempView('m_c201623_idd')
-- MAGIC #r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_idd")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623_idd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_idd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched No IDD

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_idd
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC controls as (
-- MAGIC   select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND_idd
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC control_ids as (select distinct c_medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND_idd
-- MAGIC where medrec_key in (select c_medrec_key from control_ids)
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC r.createOrReplaceTempView('m_c201623_ND_idd')
-- MAGIC #r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_ND_idd")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623_ND_idd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_ND_idd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SLDD Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### All patients with SLDD 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Premier Patients Disability - SLDD
-- MAGIC a=spark.sql("""
-- MAGIC            select distinct p.medrec_key,d.*
-- MAGIC            from PremierIDDSLDD2 d
-- MAGIC            left join (select distinct medrec_key,pat_key from cdh_premier_v2.patdemo) p  on p.pat_key=d.pat_key
-- MAGIC                     where 1=1
-- MAGIC                     and (d.sldd=1)
-- MAGIC                     order by p.medrec_key   
-- MAGIC """)
-- MAGIC a.createOrReplaceTempView('PremierDisab_sldd')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inpatients 18+ with SLDD 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2016-2023 Cohort of 18+ inpatients having disability  -SLDD                     
-- MAGIC b=spark.sql("""
-- MAGIC             select distinct medrec_key,pat_key,disc_status,disc_status_desc,los,max(age) over(partition by medrec_key) as agem, race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                  from PremierInp2 i
-- MAGIC                  where exists (select 1 from PremierDisab_sldd d
-- MAGIC                                where i.pat_key=d.pat_key
-- MAGIC                                and i.year between '2016' and '2023'
-- MAGIC                                and i.age>=18
-- MAGIC                                )
-- MAGIC                 order by medrec_key
-- MAGIC """)
-- MAGIC b.createOrReplaceTempView('c201623_sldd')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_sldd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623_sldd")

-- COMMAND ----------

select concat ('sldd = 1: ', count(distinct medrec_key))
from c201623_sldd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inpatients 18+ without SLDD 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2016-2023 Cohort of 18+ inpatients having NO disability
-- MAGIC c=spark.sql("""
-- MAGIC              select distinct medrec_key,pat_key,disc_status,disc_status_desc,los, max(age) over(partition by medrec_key) as agem, race, gender, ins, hispanic_ind, admit_date, icu, vent
-- MAGIC                       from PremierInp2 i
-- MAGIC                       where 1=1
-- MAGIC                       and i.year between '2016' and '2023'
-- MAGIC                       and i.age>=18
-- MAGIC                       and not exists(select 1 from PremierDisab_sldd d
-- MAGIC                                      where i.pat_key=d.pat_key
-- MAGIC                                     )
-- MAGIC                       order by medrec_key
-- MAGIC """)
-- MAGIC c.createOrReplaceTempView('c201623_ND_sldd')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND_sldd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_c201623_ND_sldd")

-- COMMAND ----------

select concat ('sldd = 0: ', count(*))
from c201623_ND_sldd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Matching (Use for Analysis)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched SLDD 

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC #Matched SLDD  Dataset
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_sldd
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for controls. Include maximum age and gender for matching
-- MAGIC controls as (
-- MAGIC   select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND_sldd
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC --select count(distinct medrec_key) as report from mratio
-- MAGIC
-- MAGIC --Select distinct case ids from matched participants
-- MAGIC case_ids as (select distinct medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC --Limit original dataset to matched participants
-- MAGIC select * 
-- MAGIC from c201623_sldd
-- MAGIC where medrec_key in (select medrec_key from case_ids)
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC r.createOrReplaceTempView('m_c201623_sldd')
-- MAGIC #r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_sldd")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623_sldd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_sldd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Matched No SLDD 

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission for cases. Include maximum age and gender for matching
-- MAGIC with c as (
-- MAGIC select distinct medrec_key, agem, gender, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_sldd
-- MAGIC order by medrec_key
-- MAGIC
-- MAGIC ),
-- MAGIC
-- MAGIC --Assign a unique sequential row number to distinct cases (Note: This will identify distinct cases (medrec_key))
-- MAGIC cases as (
-- MAGIC      select *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS CaseNo
-- MAGIC      from c
-- MAGIC      ),
-- MAGIC
-- MAGIC controls as (
-- MAGIC   select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND_sldd
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC
-- MAGIC --Match cases to controls by age, sex, year of admission, and quarter of admission 
-- MAGIC match as (
-- MAGIC      select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, a.CaseNo
-- MAGIC      from cases a
-- MAGIC      cross join controls b
-- MAGIC           on (a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr)), -- cross join matches each case with every possible control, sampling with replacement (Note: Controls will be matched to multiple cases)
-- MAGIC
-- MAGIC
-- MAGIC --Remove control duplicates so each control can only be matched to one case
-- MAGIC all_matchids as (
-- MAGIC   select *
-- MAGIC           from (
-- MAGIC             select *, row_number () over (partition by c_medrec_key order by rand()) as rn
-- MAGIC             from match) --for each control (c_medrec_key) randomly order cases(medrec_key/caseno) and assign a sequential number as rn
-- MAGIC           where rn = 1 --select the first row/case for each control
-- MAGIC           order by caseno), 
-- MAGIC
-- MAGIC --After this step, one case may have multiple controls, but all controls will be distinct 
-- MAGIC
-- MAGIC
-- MAGIC --Example of 1:5 match
-- MAGIC mratio as (select *
-- MAGIC from (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr, caseno, row_number() over (partition by caseno order by rand()) as rn  -- order controls randomly by case and assign a sequential number as rn
-- MAGIC from all_matchids)
-- MAGIC where rn <= 5 -- pick 5 controls for each case
-- MAGIC order by caseno),
-- MAGIC
-- MAGIC control_ids as (select distinct c_medrec_key
-- MAGIC from mratio)
-- MAGIC
-- MAGIC select * 
-- MAGIC from c201623_ND_sldd
-- MAGIC where medrec_key in (select c_medrec_key from control_ids)
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC r.createOrReplaceTempView('m_c201623_ND_sldd')
-- MAGIC #r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_ND_sldd")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC select * 
-- MAGIC from m_c201623_ND_sldd
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC r.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_m_c201623_ND_sldd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Principal ICD_Code/CCSR Diagnosis Code Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC     # Principal Dx assesment of patients COMMON CODE FOR IDD/SLDD, IDD, SLDD 
-- MAGIC d=spark.sql("""
-- MAGIC             select distinct
-- MAGIC             medrec_key,d.pat_key,icd_code,CCSR_Category,CCSR_Category_Description 
-- MAGIC             from (select pat_key,replace(icd_code,'.',"") as icd_code,icd_pri_sec from cdh_premier_V2.paticd_diag
-- MAGIC                   ) d --use replace statement to have consistent formatting in cdh premier as cdh reference
-- MAGIC             left join(select distinct medrec_key,pat_key from cdh_premier_v2.patdemo) p on p.pat_key=d.pat_key
-- MAGIC             left join (select `ICD-10-CM_Code` as ICD10, CCSR_Category,CCSR_Category_Description from 
-- MAGIC                        cdh_reference_data.ccsr_codelist
-- MAGIC                       )c on c.ICD10=d.icd_code
-- MAGIC             where ICD_PRI_SEC='P' -- 'P' selects primary icd code
-- MAGIC """)
-- MAGIC #d.createOrReplaceTempView('pdx')
-- MAGIC d.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.xud4_pdx")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 30 Day Readmission Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Readmission -30 day readmissions COMMON CODE FOR IDD/SLDD, IDD, SLDD 
-- MAGIC e=spark.sql("""
-- MAGIC               select *
-- MAGIC               from (select medrec_key,pat_key, admit_date, discharge_date, date_diff(admit_date, min(discharge_date) over (partition by medrec_key)) as days_from_index
-- MAGIC               from cdh_premier_v2.patdemo)
-- MAGIC               where days_from_index between 1 and 30         
-- MAGIC               order by medrec_key, admit_date
-- MAGIC
-- MAGIC               
-- MAGIC """)
-- MAGIC #e.createOrReplaceTempView('t30rdm')
-- MAGIC e.write.mode('overwrite').saveAsTable('cdh_premier_exploratory.xud4_t30rdm')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### AHRQ PQI Indicators

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC f = spark.sql("""
-- MAGIC --AHRQ PQI Definitions
-- MAGIC with a as (select a.medrec_key, 
-- MAGIC                   a.pat_key,
-- MAGIC                   a.point_of_origin, 
-- MAGIC                   a.gender, 
-- MAGIC                   a.age, 
-- MAGIC                   a.ms_drg, 
-- MAGIC                   b.icd_code,
-- MAGIC                   b.icd_pri_sec,
-- MAGIC
-- MAGIC                   case 
-- MAGIC                       when icd_code in ('J13', 'J14', 'J15.211', 'J15.212', 'J15.3', 'J15.4', 'J15.7', 'J15.9', 'J16.0', 'J16.8', 'J18.0', 'J18.1', 'J18.8', 'J18.9') and b.ICD_PRI_SEC = "P" then 1 
-- MAGIC                       else 0 
-- MAGIC                   end as pneu, --primary community-acquired bacterial pneumonia diagnosis
-- MAGIC
-- MAGIC                   case
-- MAGIC                     when icd_code in ('N28.86','N30.00','N30.01','N30.90','N30.91','N39.0','N10','N12','N15.1','N15.9','N16','N28.84','N28.85') and b.ICD_PRI_SEC = "P" then 1
-- MAGIC                     else 0
-- MAGIC                   end as uti, --primary uti diagnosis
-- MAGIC
-- MAGIC                   case
-- MAGIC                     when icd_code in ('E10.649','E10.65','E11.649','E11.65','E13.649','E13.65') and b.ICD_PRI_SEC = 'P' then 1
-- MAGIC                     else 0
-- MAGIC                   end as diabetes --primary uncontrolled diabetes diagnosis
-- MAGIC
-- MAGIC             from edav_prd_cdh.cdh_premier_v2.patdemo a
-- MAGIC             left join edav_prd_cdh.cdh_premier_v2.paticd_diag b 
-- MAGIC             on a.pat_key = b.pat_key),
-- MAGIC
-- MAGIC --Inclusion: ages 18 years or older
-- MAGIC include as (select *
-- MAGIC             from a
-- MAGIC             where age >=18),
-- MAGIC
-- MAGIC --Exclusion
-- MAGIC
-- MAGIC exclude as (
-- MAGIC             select * 
-- MAGIC             from
-- MAGIC             (select a.medrec_key, 
-- MAGIC              a.pat_key, 
-- MAGIC              a.point_of_origin,
-- MAGIC              case 
-- MAGIC                 when b.icd_code_diag in ('D57.00', 'D57.01', 'D57.02','D57.1', 'D57.20','D57.211','D57.212','D57.213','D57.218','D57.219','D57.40', 'D57.411', 'D57.412', 'D57.413', 'D57.418', 'D57.419', 'D57.42', 'D57.431', 'D57.432', 'D57.433', 'D57.438', 'D57.439', 'D57.44', 'D57.451', 'D57.452', 'D57.453','D57.458', 'D57.459', 'D57.80', 'D57.811', 'D57.812', 'D57.813', 'D57.818', 'D57.819') then 1
-- MAGIC                 else 0
-- MAGIC               end as sickle,
-- MAGIC
-- MAGIC               case 
-- MAGIC                 when b.icd_code_diag in ('B20', 'B59', 'C80.2', 'C88.8', 'C94.40', 'C94.41','C94.42','C94.6','D46.22','D47.01',	'D47.02','D47.09','D47.1','D47.9','D47.Z1','D47.Z2','D47.Z9','D61.09','D61.810','D61.811','D61.818','D70.0','D70.1',	'D70.2', 'D70.4', 'D70.8', 'D70.9', 'D71', 'D72.0', 'D72.810', 'D72.818', 'D72.819', 'D73.81', 'D75.81', 	'D76.1', 'D76.2','D76.3','D80.0','D80.1','D80.2','D80.3','D80.4','D80.5','D80.6','D80.7','D80.8',	'D80.9','D81.0',	'D81.1',	'D81.2','D81.30','D81.31','D81.32','D81.39',	'D81.4','D81.6','D81.7','D81.82','D81.89','D81.9','D82.0','D82.1','D82.2','D82.3','D82.4','D82.8','D82.9','D83.0','D83.1','D83.2','D83.8','D83.9','D84.0', 'D84.1', 'D84.8','D84.81','D84.821','D84.822','D84.89','D84.9','D89.3','D89.810','D89.811','D89.812', 'D89.813','D89.82', 'D89.89', 'D89.9', 'E40','E41', 'E42','E43','I12.0', 'I13.11', 'I13.2', 'K91.2','N18.5','N18.6','T86.00','T86.01','T86.02','T86.03','T86.09','T86.10','T86.11','T86.12','T86.13','T86.19','T86.20','T86.21','T86.22','T86.23', 'T86.290','T86.298','T86.30','T86.31','T86.32','T86.33','T86.39','T86.40','T86.41','T86.42','T86.43','T86.49','T86.5','T86.810', 'T86.811', 'T86.812','T86.818','T86.819','T86.850','T86.851','T86.852','T86.858','T86.859','T86.890','T86.891','T86.892','T86.898','T86.899','T86.90','T86.91','T86.92', 'T86.93','T86.99','Z48.21','Z48.22','Z48.23','Z48.24','Z48.280','Z48.288','Z48.290','Z48.298','Z49.01','Z49.02','Z49.31','Z49.32','Z94.0','Z94.1','Z94.2','Z94.3','Z94.4','Z94.81','Z94.82','Z94.83','Z94.84','Z94.89','Z99.2') then 1
-- MAGIC                 else 0
-- MAGIC               end as immunid,
-- MAGIC
-- MAGIC               case
-- MAGIC                 when c.icd_code_proc in ('02YA0Z0','02YA0Z2','0BYC0Z0', '0BYC0Z2', '0BYD0Z0','0BYD0Z2', '0BYF0Z0','0BYF0Z2','0BYG0Z0','0BYG0Z2','0BYH0Z0','0BYH0Z2','0BYJ0Z0','0BYJ0Z2','0BYK0Z0', '0BYK0Z2','0BYL0Z0','0BYL0Z2','0BYM0Z0','0BYM0Z2','0DY50Z0','0DY50Z2','0DY60Z0','0DY60Z2','0DY80Z0', '0DY80Z2','0DYE0Z0','0DYE0Z2','0FY00Z0','0FY00Z2','0FYG0Z0','0FYG0Z2','0TY00Z0','0TY00Z2','0TY10Z0','0TY10Z2','0WY20Z0','0XYJ0Z0','0XYK0Z0','30230AZ','30230G0', '30230G1','30230G2','30230G3','30230G4','30230U2','30230U3','30230U4','30230X0','30230X1','30230X2','30230X3','30230X4','30230Y0','30230Y1','30230Y2','30230Y3','30230Y4','30233AZ','30233G0','30233G1','30233G2','30233G3','30233G4','30233U2','30233U3','30233U4','30233X0','30233X1','30233X2','30233X3','30233X4','30233Y0','30233Y1','30233Y2','30233Y3','30233Y4','30240AZ','30240G0','30240G1','30240G2', '30240G3','30240G4','30240U2','30240U3','30240U4','30240X0','30240X1','30240X2','30240X3','30240X4','30240Y0','30240Y1','30240Y2','30240Y3','30240Y4','30243AZ','30243G0','30243G1','30243G2','30243G3','30243G4','30243U2','30243U3','30243U4','30243X0','30243X1','30243X2','30243X3','30243X4','30243Y0','30243Y1','30243Y2','30243Y3','30243Y4','3E03005','3E0300P','3E030U1','3E030WL','3E03305','3E0330P','3E033U1','3E033WL','3E04005','3E0400P','3E040WL','3E04305','3E0430P','3E043WL','3E0A305','3E0J3U1','3E0J7U1','3E0J8U1','XW01318','XW01348','XW03336','XW03351','XW03358','XW03368','XW03378','XW03387','XW03388','XW033B3','XW033C6','XW033D6','XW033H7','XW033J7','XW033K7','XW033M7','XW033N7','XW033S5','XW04336','XW04351','XW04358','XW04368','XW04378','XW04387','XW04388','XW043B3','XW043C6','XW043D6','XW043H7','XW043J7','XW043K7','XW043M7','XW043N7','XW043S5','XW133B8', 'XW143B8','XW143C8','XW23346','XW23376','XW24346','XW24376','XW133C8') then 1
-- MAGIC                 else 0
-- MAGIC               end as immunip
-- MAGIC           
-- MAGIC           from edav_prd_cdh.cdh_premier_v2.patdemo a
-- MAGIC           left join (select pat_key, icd_code as icd_code_diag from edav_prd_cdh.cdh_premier_v2.paticd_diag) b 
-- MAGIC           on a.pat_key = b.pat_key
-- MAGIC           left join (select pat_key, icd_code as icd_code_proc from edav_prd_cdh.cdh_premier_v2.paticd_proc) c
-- MAGIC           on a.pat_key = c.pat_key)
-- MAGIC
-- MAGIC           where sickle = 1 OR immunid = 1 OR immunip = 1)
-- MAGIC   
-- MAGIC   -- Remove exclusions from include dataset
-- MAGIC   select *
-- MAGIC   from include
-- MAGIC   where 1=1
-- MAGIC         and point_of_origin not in (2,3,4,5,6) 
-- MAGIC         and ms_drg != 999 
-- MAGIC         and pat_key not in (select pat_key from exclude)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC #f.createOrReplaceTempView('ahrq')	
-- MAGIC f.write.mode('overwrite').saveAsTable('cdh_premier_exploratory.xud4_ahrq_pqi')
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #"Exclude discharges:
-- MAGIC #• with any listed ICD-10-CM diagnosis code for sickle cell or Hemoglobin S disease (ACSBA2D* )
-- MAGIC #• with any listed ICD-10-CM diagnosis code for immunocompromised state (Appendix C: IMMUNID ) or any listed ICD-10-PCS procedure code for immunocompromised state (Appendix C: IMMUNIP )
-- MAGIC #• with admission source for transferred from a different hospital or other health care facility (Appendix A ) (UB04 Admission source - 2, 3)
-- MAGIC #• with a point of origin code for transfer from a hospital, skilled nursing facility (SNF) or intermediate care facility (ICF), or other healthcare facility (Appendix A ) (UB04 Point of Origin - 4, 5, 6)
-- MAGIC #• with an ungroupable DRG (DRG=999)
-- MAGIC #• with missing gender (SEX=missing), age (AGE=missing), quarter (DQTR=missing), year (YEAR=missing), principal diagnosis (DX1=missing), or county (PSTCO=missing)"	
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### AHRQ PSI Indicators
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %python
-- MAGIC g = spark.sql("""
-- MAGIC
-- MAGIC --AHRQ PSI Definitions
-- MAGIC with a as (select a.medrec_key, 
-- MAGIC                   a.pat_key,   
-- MAGIC                   a.point_of_origin, 
-- MAGIC                   a.gender, 
-- MAGIC                   a.age, 
-- MAGIC                   a.ms_drg,
-- MAGIC                   a.disc_status,
-- MAGIC                   b.icd_code,
-- MAGIC                   b.icd_pri_sec,
-- MAGIC                    case 
-- MAGIC                       when a.ms_drg in (052,053,069,075,076,102,103,113,114,115,116,117,121,122,123,135,136,137,138,139,149,202,203,312,313,483,488,489,506,507,508,513,514,537,538,582,583,584,585,600,601,614,615,671,672,697,707,708,709,710,711,712,713,714,717,718,729,730,734,735,742,743,746,747,748,760,761,864,876,880,881,882,883,885,886,887,904,905,906,945,946,974,975,976) then 1
-- MAGIC                       else 0
-- MAGIC                   end as lowmodr
-- MAGIC             from edav_prd_cdh.cdh_premier_v2.patdemo a
-- MAGIC             left join edav_prd_cdh.cdh_premier_v2.paticd_diag b 
-- MAGIC             on a.pat_key = b.pat_key),
-- MAGIC
-- MAGIC --Inclusion: ages 18 years or older
-- MAGIC include as (select *
-- MAGIC             from a
-- MAGIC             where age >=18),
-- MAGIC
-- MAGIC --Exclusion
-- MAGIC
-- MAGIC exclude as (
-- MAGIC
-- MAGIC select * 
-- MAGIC from (                
-- MAGIC select a.medrec_key, 
-- MAGIC        a.pat_key, 
-- MAGIC        a.point_of_origin,
-- MAGIC  
-- MAGIC
-- MAGIC case 
-- MAGIC   when b.icd_code in ('M9910','S62151A','M9911','S62151B','M9912','S62152A','M9913','S62152B','M9914','S62153A','M9915','S62153B','M9916','S62154A','M9917','S62154B','M9918','S62155A','M9919','S62155B','O9A211','S62156A','O9A212','S62156B','O9A213','S62161A','O9A219','S62161B','O9A22','S62162A','O9A23','S62162B','O9A311','S62163A','O9A312','S62163B','O9A313','S62164A','O9A319','S62164B','O9A32','O9A33','S62165B','S0100XA','S62166A','S0101XA','S62166B','S0102XA','S62171A','S0103XA','S62171B','S0104XA','S62172A','S0105XA','S01101A','S62173A','S01102A','S62173B','S01109A','S62174A','S01111A','S62174B','S01112A','S62175A','S01119A','S62175B','S01121A','S62176A','S01122A','S62176B','S01129A','S62181A','S01131A','S62181B','S01132A','S62182A','S01139A','S62182B','S01141A','S62183A','S01142A','S62183B','S01149A','S62184A','S01151A','S62184B','S01152A','S62185A','S01159A','S62185B','S0120XA','S62186A','S0121XA','S62186B','S0122XA','S62201A','S0123XA','S62201B','S0124XA','S62202A','S0125XA','S01301A','S62209A','S01302A','S62209B','S01309A','S62211A','S01311A','S62211B','S01312A','S62212A','S01319A','S62212B','S01321A','S62213A','S01322A','S62213B','S01329A','S62221A','S01331A','S62221B','S01332A','S62222A','S01339A','S62222B','S01341A','S62223A','S01342A','S62223B','S01349A','S62224A','S01351A','S01352A','S01359A','S62225B','S01401A','S62226A','S01402A','S62226B','S01409A','S62231A','S01411A','S62231B','S01412A','S62232A','S01419A','S62232B','S01421A','S62233A','S01422A','S62233B','S01429A','S62234A','S01431A','S62234B','S01432A','S62235A','S01439A','S62235B','S01441A','S62236A','S01442A','S62236B','S01449A','S62241A','S01451A','S62241B','S01452A','S62242A','S01459A','S62242B','S01501A','S62243A','S01502A','S62243B','S01511A','S62244A','S01512A','S62244B','S01521A','S62245A','S01522A','S62245B','S01531A','S62246A','S01532A','S62246B','S01541A','S62251A','S01542A','S62251B','S01551A','S01552A','S0180XA','S62253A','S0181XA','S62253B','S0182XA','S62254A','S0183XA','S62254B','S0184XA','S62255A','S0185XA','S62255B','S0190XA','S62256A','S0191XA','S62256B','S0192XA','S62291A','S0193XA','S62291B','S0194XA','S62292A','S0195XA','S62292B','S020XXA','S62299A','S020XXB','S62299B','S02101A','S62300A','S02101B','S62300B','S02102A','S62301A','S02102B','S62301B','S02109A','S62302A','S02109B','S62302B','S0210XA','S62303A','S0210XB','S62303B','S02110A','S62304A','S02110B','S62304B','S02111A','S62305A','S02111B','S62305B','S02112A','S62306A','S02112B','S62306B','S02113A','S62307A','S02113B','S62307B','S02118A','S62308A','S02118B','S62308B','S02119A','S62309A','S02119B','S62309B','S0211AA','S62310A','S0211AB','S62310B','S0211BA','S62311A','S0211BB','S62311B','S0211CA','S62312A','S0211CB','S62312B','S0211DA','S62313A','S0211DB','S62313B','S0211EA','S62314A','S0211EB','S62314B','S0211FA','S62315A','S0211FB','S62315B','S0211GA','S62316A','S0211GB','S62316B','S0211HA','S62317A','S0211HB','S62317B','S02121A','S62318A','S02121B','S62318B','S02122A','S62319A','S02122B','S62319B','S02129A','S62320A','S02129B','S62320B','S0219XA','S62321A','S0219XB','S62321B','S022XXA','S62322A','S022XXB','S62322B','S0230XA','S62323A','S0230XB','S62323B','S0231XA','S62324A','S0231XB','S62324B','S0232XA','S62325A','S0232XB','S62325B','S023XXA','S62326A','S023XXB','S62326B','S02400A','S62327A','S02400B','S62327B','S02401A','S62328A','S02401B','S62328B','S02402A','S62329A','S02402B','S62329B','S0240AA','S62330A','S0240AB','S62330B','S0240BA','S62331A','S0240BB','S62331B','S0240CA','S62332A','S0240CB','S62332B','S0240DA','S62333A','S0240DB','S62333B','S0240EA','S62334A','S0240EB','S62334B','S0240FA','S62335A','S0240FB','S62335B','S02411A','S62336A','S02411B','S62336B','S02412A','S62337A','S02412B','S62337B','S02413A','S62338A','S02413B','S62338B','S0242XA','S62339A','S0242XB','S62339B','S025XXA','S62340A','S025XXB','S62340B','S02600A','S62341A','S02600B','S62341B','S02601A','S62342A','S02601B','S62342B','S02602A','S62343A','S02602B','S62343B','S02609A','S62344A','S02609B','S62344B','S02610A','S62345A','S02610B','S62345B','S02611A','S62346A','S02611B','S62346B','S02612A','S62347A','S02612B','S62347B','S0261XA','S62348A','S0261XB','S62348B','S02620A','S62349A','S02620B','S62349B','S02621A','S62350A','S02621B','S62350B','S02622A','S62351A','S02622B','S62351B','S0262XA','S62352A','S0262XB','S62352B','S02630A','S62353A','S02630B','S62353B','S02631A','S62354A','S02631B','S62354B','S02632A','S62355A','S02632B','S62355B','S0263XA','S62356A','S0263XB','S62356B','S02640A','S62357A','S02640B','S62357B','S02641A','S62358A','S02641B','S62358B','S02642A','S62359A','S02642B','S62359B','S0264XA','S62360A','S0264XB','S62360B','S02650A','S62361A','S02650B','S62361B','S02651A','S62362A','S02651B','S62362B','S02652A','S62363A','S02652B','S62363B','S0265XA','S62364A','S0265XB','S62364B','S0266XA','S62365A','S0266XB','S62365B','S02670A','S62366A','S02670B','S62366B','S02671A','S62367A','S02671B','S62367B','S02672A','S62368A','S02672B','S62368B','S0267XA','S62369A','S0267XB','S62369B','S0269XA','S62390A','S0269XB','S62390B','S0280XA','S62391A','S0280XB','S62391B','S0281XA','S62392A','S0281XB','S62392B','S0282XA','S62393A','S0282XB','S62393B','S02831A','S62394A','S02831B','S62394B','S02832A','S62395A','S02832B','S62395B','S02839A','S62396A','S02839B','S62396B','S02841A','S62397A','S02841B','S62397B','S02842A','S62398A','S02842B','S62398B','S02849A','S62399A','S02849B','S62399B','S0285XA','S6290XA','S0285XB','S6290XB','S028XXA','S63001A','S028XXB','S63002A','S0291XA','S63003A','S0291XB','S63004A','S0292XA','S63005A','S0292XB','S63006A','S0300XA','S63011A','S0301XA','S63012A','S0302XA','S63013A','S0303XA','S63014A','S030XXA','S032XXA','S0520XA','S63021A','S0521XA','S63022A','S0522XA','S63023A','S0530XA','S63024A','S0531XA','S63025A','S0532XA','S63026A','S0540XA','S63031A','S0541XA','S63032A','S0542XA','S63033A','S0550XA','S63034A','S0551XA','S63035A','S0552XA','S63036A','S0560XA','S63041A','S0561XA','S63042A','S0562XA','S63043A','S0570XA','S63044A','S0571XA','S0572XA','S058X1A','S63051A','S058X2A','S63052A','S058X9A','S63053A','S0590XA','S63054A','S0591XA','S63055A','S0592XA','S63056A','S060X0A','S63061A','S060X1A','S63062A','S060X2A','S63063A','S060X3A','S63064A','S060X4A','S63065A','S060X5A','S63066A','S060X6A','S63071A','S060X7A','S63072A','S060X8A','S63073A','S060X9A','S63074A','S060XAA','S63075A','S061X0A','S63076A','S061X1A','S63091A','S061X2A','S63092A','S061X3A','S63093A','S061X4A','S63094A','S061X5A','S63095A','S061X6A','S63096A','S061X7A','S65001A','S061X8A','S65002A','S061X9A','S65009A','S061XAA','S65011A','S062X0A','S65012A','S062X1A','S65019A','S062X2A','S65091A','S062X3A','S65092A','S062X4A','S65099A','S062X5A','S65101A','S062X6A','S65102A','S062X7A','S65109A','S062X8A','S65111A','S062X9A','S65112A','S062XAA','S65119A','S06300A','S65191A','S06301A','S65192A','S06302A','S65199A','S06303A','S65201A','S06304A','S65202A','S06305A','S65209A','S06306A','S65211A','S06307A','S65212A','S06308A','S65219A','S06309A','S65291A','S0630AA','S65292A','S06310A','S65299A','S06311A','S65301A','S06312A','S65302A','S06313A','S65309A','S06314A','S65311A','S06315A','S65312A','S06316A','S65319A','S06317A','S65391A','S06318A','S65392A','S06319A','S65399A','S0631AA','S65401A','S06320A','S65402A','S06321A','S65409A','S06322A','S65411A','S06323A','S65412A','S06324A','S65419A','S06325A','S65491A','S06326A','S65492A','S06327A','S65499A','S06328A','S65500A','S06329A','S65501A','S0632AA','S65502A','S06330A','S65503A','S06331A','S65504A','S06332A','S65505A','S06333A','S65506A','S06334A','S65507A','S06335A','S65508A','S06336A','S65509A','S06337A','S65510A','S06338A','S65511A','S06339A','S65512A','S0633AA','S65513A','S06340A','S65514A','S06341A','S65515A','S06342A','S65516A','S06343A','S65517A','S06344A','S65518A','S06345A','S65519A','S06346A','S65590A','S06347A','S65591A','S06348A','S65592A','S06349A','S65593A','S0634AA','S65594A','S06350A','S65595A','S06351A','S65596A','S06352A','S65597A','S06353A','S65598A','S06354A','S65599A','S06355A','S65801A','S06356A','S65802A','S06357A','S65809A','S06358A','S65811A','S06359A','S65812A','S0635AA','S65819A','S06360A','S65891A','S06361A','S65892A','S06362A','S65899A','S06363A','S65901A','S06364A','S65902A','S06365A','S65909A','S06366A','S65911A','S06367A','S65912A','S06368A','S65919A','S06369A','S65991A','S0636AA','S65992A','S06370A','S65999A','S06371A','S66021A','S06372A','S66022A','S06373A','S66029A','S06374A','S66120A','S06375A','S66121A','S06376A','S66122A','S06377A','S66123A','S06378A','S66124A','S06379A','S66125A','S0637AA','S66126A','S06380A','S66127A','S06381A','S66128A','S06382A','S66129A','S06383A','S66221A','S06384A','S66222A','S06385A','S66229A','S06386A','S66320A','S06387A','S66321A','S06388A','S66322A','S06389A','S66323A','S0638AA','S66324A','S064X0A','S66325A','S064X1A','S66326A','S064X2A','S66327A','S064X3A','S66328A','S064X4A','S66329A','S064X5A','S66421A','S064X6A','S66422A','S064X7A','S66429A','S064X8A','S66520A','S064X9A','S66521A','S064XAA','S66522A','S065X0A','S66523A','S065X1A','S66524A','S065X2A','S66525A','S065X3A','S66526A','S065X4A','S66527A','S065X5A','S66528A','S065X6A','S66529A','S065X7A','S66821A','S065X8A','S66822A','S065X9A','S66829A','S065XAA','S66921A','S066X0A','S66922A','S066X1A','S66929A','S066X2A','S6700XA','S066X3A','S6701XA','S066X4A','S6702XA','S066X5A','S6710XA','S066X6A','S67190A','S066X7A','S67191A','S066X8A','S67192A','S066X9A','S67193A','S066XAA','S67194A','S06810A','S67195A','S06811A','S67196A','S06812A','S67197A','S06813A','S67198A','S06814A','S6720XA','S06815A','S6721XA','S06816A','S6722XA','S06817A','S6730XA','S06818A','S6731XA','S06819A','S6732XA','S0681AA','S6740XA','S06820A','S6741XA','S06821A','S6742XA','S06822A','S6790XA','S06823A','S6791XA','S06824A','S6792XA','S06825A','S68411A','S06826A','S68412A','S06827A','S68419A','S06828A','S68421A','S06829A','S68422A','S0682AA','S68429A','S06890A','S68711A','S06891A','S68712A','S06892A','S68719A','S06893A','S68721A','S06894A','S68722A','S06895A','S68729A','S06896A','S71001A','S06897A','S71002A','S06898A','S71009A','S06899A','S71011A','S0689AA','S71012A','S068A0A','S71019A','S068A1A','S71021A','S068A2A','S71022A','S068A3A','S71029A','S068A4A','S71031A','S068A5A','S71032A','S068A6A','S71039A','S068A7A','S71041A','S068A8A','S71042A','S068A9A','S71049A','S068AAA','S71051A','S069X0A','S71052A','S069X1A','S71059A','S069X2A','S71101A','S069X3A','S71102A','S069X4A','S71109A','S069X5A','S71111A','S069X6A','S71112A','S069X7A','S71119A','S069X8A','S71121A','S069X9A','S71122A','S069XAA','S71129A','S06A0XA','S71131A','S06A1XA','S71132A','S070XXA','S071XXA','S078XXA','S71142A','S079XXA','S71149A','S080XXA','S08111A','S71152A','S08112A','S71159A','S08119A','S72001A','S08121A','S72001B','S08122A','S72001C','S08129A','S72002A','S08811A','S72002B','S08812A','S72002C','S0889XA','S72009A','S090XXA','S72009B','S0912XA','S72009C','S0920XA','S72011A','S0921XA','S72011B','S0922XA','S72011C','S09301A','S72012A','S09302A','S72012B','S09309A','S72012C','S09311A','S72019A','S09312A','S72019B','S09313A','S72019C','S09319A','S72021A','S09391A','S72021B','S09392A','S72021C','S09399A','S72022A','S098XXA','S72022B','S0990XA','S72022C','S0991XA','S0993XA','S72023B','S11011A','S72023C','S11012A','S72024A','S11013A','S72024B','S11014A','S72024C','S11015A','S11019A','S72025B','S11021A','S72025C','S11022A','S72026A','S11023A','S72026B','S11024A','S72026C','S11025A','S11029A','S72031B','S11031A','S72031C','S11032A','S72032A','S11033A','S72032B','S11034A','S72032C','S11035A','S11039A','S72033B','S1110XA','S72033C','S1111XA','S72034A','S1112XA','S72034B','S1113XA','S72034C','S1114XA','S72035A','S1115XA','S72035B','S1120XA','S72035C','S1121XA','S72036A','S1122XA','S72036B','S1123XA','S72036C','S1124XA','S72041A','S1125XA','S72041B','S1180XA','S72041C','S1181XA','S72042A','S1182XA','S72042B','S1183XA','S72042C','S1184XA','S72043A','S1185XA','S72043B','S1189XA','S72043C','S1190XA','S72044A','S1191XA','S72044B','S1192XA','S72044C','S1193XA','S72045A','S1194XA','S72045B','S1195XA','S72045C','S12000A','S72046A','S12000B','S72046B','S12001A','S72046C','S12001B','S72051A','S1201XA','S72051B','S1201XB','S72051C','S1202XA','S72052A','S1202XB','S72052B','S12030A','S72052C','S12030B','S72059A','S12031A','S72059B','S12031B','S72059C','S12040A','S72061A','S12040B','S72061B','S12041A','S72061C','S12041B','S72062A','S12090A','S72062B','S12090B','S72062C','S12091A','S72063A','S12091B','S72063B','S12100A','S72063C','S12100B','S72064A','S12101A','S72064B','S12101B','S72064C','S12110A','S72065A','S12110B','S72065B','S12111A','S72065C','S12111B','S72066A','S12112A','S72066B','S12112B','S72066C','S12120A','S72091A','S12120B','S72091B','S12121A','S72091C','S12121B','S72092A','S12130A','S72092B','S12130B','S72092C','S12131A','S72099A','S12131B','S72099B','S1214XA','S72099C','S1214XB','S72101A','S12150A','S72101B','S12150B','S72101C','S12151A','S72102A','S12151B','S72102B','S12190A','S72102C','S12190B','S72109A','S12191A','S72109B','S12191B','S72109C','S12200A','S72111A','S12200B','S72111B','S12201A','S72111C','S12201B','S72112A','S12230A','S72112B','S12230B','S72112C','S12231A','S72113A','S12231B','S72113B','S1224XA','S72113C','S1224XB','S72114A','S12250A','S72114B','S12250B','S72114C','S12251A','S72115A','S12251B','S72115B','S12290A','S72115C','S12290B','S72116A','S12291A','S72116B','S12291B','S72116C','S12300A','S72121A','S12300B','S72121B','S12301A','S72121C','S12301B','S72122A','S12330A','S72122B','S12330B','S72122C','S12331A','S72123A','S12331B','S72123B','S1234XA','S72123C','S1234XB','S72124A','S12350A','S72124B','S12350B','S72124C','S12351A','S72125A','S12351B','S72125B','S12390A','S72125C','S12390B','S72126A','S12391A','S72126B','S12391B','S72126C','S12400A','S72131A','S12400B','S72131B','S12401A','S72131C','S12401B','S72132A','S12430A','S72132B','S12430B','S72132C','S12431A','S72133A','S12431B','S72133B','S1244XA','S72133C','S1244XB','S72134A','S12450A','S72134B','S12450B','S72134C','S12451A','S72135A','S12451B','S72135B','S12490A','S72135C','S12490B','S72136A','S12491A','S72136B','S12491B','S72136C','S12500A','S72141A','S12500B','S72141B','S12501A','S72141C','S12501B','S72142A','S12530A','S72142B','S12530B','S72142C','S12531A','S72143A','S12531B','S72143B','S1254XA','S72143C','S1254XB','S72144A','S12550A','S72144B','S12550B','S72144C','S12551A','S72145A','S12551B','S72145B','S12590A','S72145C','S12590B','S72146A','S12591A','S72146B','S12591B','S72146C','S12600A','S7221XA','S12600B','S7221XB','S12601A','S7221XC','S12601B','S7222XA','S12630A','S7222XB','S12630B','S7222XC','S12631A','S7223XA','S12631B','S7223XB','S1264XA','S7223XC','S1264XB','S7224XA','S12650A','S7224XB','S12650B','S7224XC','S12651A','S7225XA','S12651B','S7225XB','S12690A','S7225XC','S12690B','S7226XA','S12691A','S7226XB','S12691B','S7226XC','S128XXA','S72301A','S129XXA','S72301B','S130XXA','S72301C','S13100A','S72302A','S13101A','S72302B','S13110A','S72302C','S13111A','S72309A','S13120A','S72309B','S13121A','S72309C','S13130A','S72321A','S13131A','S72321B','S13140A','S72321C','S13141A','S72322A','S13150A','S72322B','S13151A','S72322C','S13160A','S72323A','S13161A','S72323B','S13170A','S72323C','S13171A','S72324A','S13180A','S72324B','S13181A','S72324C','S1320XA','S72325A','S1329XA','S72325B','S140XXA','S72325C','S14101A','S72326A','S14102A','S72326B','S14103A','S72326C','S14104A','S72331A','S14105A','S72331B','S14106A','S72331C','S14107A','S72332A','S14108A','S72332B','S14109A','S72332C','S14111A','S72333A','S14112A','S72333B','S14113A','S72333C','S14114A','S72334A','S14115A','S72334B','S14116A','S72334C','S14117A','S72335A','S14118A','S72335B','S14119A','S72335C','S14121A','S72336A','S14122A','S72336B','S14123A','S72336C','S14124A','S72341A','S14125A','S72341B','S14126A','S72341C','S14127A','S72342A','S14128A','S72342B','S14129A','S72342C','S14131A','S72343A','S14132A','S72343B','S14133A','S72343C','S14134A','S72344A','S14135A','S72344B','S14136A','S72344C','S14137A','S72345A','S14138A','S72345B','S14139A','S72345C','S14141A','S72346A','S14142A','S72346B','S14143A','S72346C','S14144A','S72351A','S14145A','S72351B','S14146A','S72351C','S14147A','S72352A','S14148A','S72352B','S14149A','S72352C','S14151A','S72353A','S14152A','S72353B','S14153A','S72353C','S14154A','S72354A','S14155A','S72354B','S14156A','S72354C','S14157A','S72355A','S14158A','S72355B','S14159A','S72355C','S142XXA','S72356A','S143XXA','S15001A','S72356C','S15002A','S72361A','S15009A','S72361B','S15011A','S72361C','S15012A','S72362A','S15019A','S72362B','S15021A','S72362C','S15022A','S72363A','S15029A','S72363B','S15091A','S72363C','S15092A','S72364A','S15099A','S72364B','S15101A','S72364C','S15102A','S72365A','S15109A','S72365B','S15111A','S72365C','S15112A','S72366A','S15119A','S72366B','S15121A','S72366C','S15122A','S72391A','S15129A','S72391B','S15191A','S72391C','S15192A','S72392A','S15199A','S72392B','S15201A','S72392C','S15202A','S72399A','S15209A','S72399B','S15211A','S72399C','S15212A','S72401A','S15219A','S72401B','S15221A','S72401C','S15222A','S72402A','S15229A','S72402B','S15291A','S72402C','S15292A','S72409A','S15299A','S72409B','S15301A','S72409C','S15302A','S72411A','S15309A','S72411B','S15311A','S72411C','S15312A','S72412A','S15319A','S72412B','S15321A','S72412C','S15322A','S72413A','S15329A','S72413B','S15391A','S72413C','S15392A','S72414A','S15399A','S72414B','S158XXA','S72414C','S159XXA','S72415A','S162XXA','S72415B','S170XXA','S72415C','S178XXA','S72416A','S179XXA','S72416B','S21001A','S72416C','S21002A','S72421A','S21009A','S72421B','S21011A','S72421C','S21012A','S72422A','S21019A','S72422B','S21021A','S72422C','S21022A','S72423A','S21029A','S72423B','S21031A','S72423C','S21032A','S72424A','S21039A','S72424B','S21041A','S72424C','S21042A','S72425A','S21049A','S72425B','S21051A','S21052A','S21059A','S72426B','S21101A','S72426C','S21102A','S72431A','S21109A','S72431B','S21111A','S72431C','S21112A','S72432A','S21119A','S72432B','S21121A','S72432C','S21122A','S72433A','S21129A','S72433B','S21131A','S72433C','S21132A','S72434A','S21139A','S72434B','S21141A','S72434C','S21142A','S72435A','S21149A','S72435B','S21151A','S72435C','S21152A','S72436A','S21159A','S72436B','S21201A','S72436C','S21202A','S72441A','S21209A','S72441B','S21211A','S72441C','S21212A','S72442A','S21219A','S72442B','S21221A','S72442C','S21222A','S72443A','S21229A','S72443B','S21231A','S72443C','S21232A','S72444A','S21239A','S72444B','S21241A','S72444C','S21242A','S72445A','S21249A','S72445B','S21251A','S72445C','S21252A','S72446A','S21259A','S72446B','S21301A','S72446C','S21302A','S72451A','S21309A','S72451B','S21311A','S72451C','S21312A','S72452A','S21319A','S72452B','S21321A','S72452C','S21322A','S72453A','S21329A','S72453B','S21331A','S72453C','S21332A','S72454A','S21339A','S72454B','S21341A','S72454C','S21342A','S72455A','S21349A','S72455B','S21351A','S72455C','S21352A','S72456A','S21359A','S72456B','S21401A','S72456C','S21402A','S72461A','S21409A','S72461B','S21411A','S72461C','S21412A','S72462A','S21419A','S72462B','S21421A','S72462C','S21422A','S72463A','S21429A','S72463B','S21431A','S72463C','S21432A','S72464A','S21439A','S72464B','S21441A','S72464C','S21442A','S72465A','S21449A','S72465B','S21451A','S72465C','S21452A','S72466A','S21459A','S72466B','S2190XA','S72466C','S2191XA','S72471A','S2192XA','S72472A','S2193XA','S72479A','S2194XA','S72491A','S2195XA','S72491B','S22000A','S72491C','S22000B','S72492A','S22001A','S72492B','S22001B','S72492C','S22002A','S72499A','S22002B','S72499B','S22008A','S72499C','S22008B','S728X1A','S22009A','S728X1B','S22009B','S728X1C','S22010A','S728X2A','S22010B','S728X2B','S22011A','S728X2C','S22011B','S728X9A','S22012A','S728X9B','S22012B','S728X9C','S22018A','S7290XA','S22018B','S7290XB','S22019A','S7290XC','S22019B','S7291XA','S22020A','S7291XB','S22020B','S7291XC','S22021A','S7292XA','S22021B','S7292XB','S22022A','S7292XC','S22022B','S73001A','S22028A','S73002A','S22028B','S73003A','S22029A','S73004A','S22029B','S73005A','S22030A','S73006A','S22030B','S73011A','S22031A','S73012A','S22031B','S73013A','S22032A','S73014A','S22032B','S73015A','S22038A','S73016A','S22038B','S73021A','S22039A','S73022A','S22039B','S73023A','S22040A','S73024A','S22040B','S73025A','S22041A','S73026A','S22041B','S73031A','S22042A','S73032A','S22042B','S73033A','S22048A','S73034A','S22048B','S73035A','S22049A','S73036A','S22049B','S73041A','S22050A','S73042A','S22050B','S73043A','S22051A','S73044A','S22051B','S73045A','S22052A','S73046A','S22052B','S75001A','S22058A','S75002A','S22058B','S75009A','S22059A','S75011A','S22059B','S75012A','S22060A','S75019A','S22060B','S75021A','S22061A','S75022A','S22061B','S75029A','S22062A','S75091A','S22062B','S75092A','S22068A','S75099A','S22068B','S75101A','S22069A','S75102A','S22069B','S75109A','S22070A','S75111A','S22070B','S75112A','S22071A','S75119A','S22071B','S75121A','S22072A','S75122A','S22072B','S75129A','S22078A','S75191A','S22078B','S75192A','S22079A','S75199A','S22079B','S75201A','S22080A','S75202A','S22080B','S75209A','S22081A','S75211A','S22081B','S75212A','S22082A','S75219A','S22082B','S75221A','S22088A','S75222A','S22088B','S75229A','S22089A','S75291A','S22089B','S75292A','S2220XA','S75299A','S2220XB','S75801A','S2221XA','S75802A','S2221XB','S75809A','S2222XA','S75811A','S2222XB','S75812A','S2223XA','S75819A','S2223XB','S75891A','S2224XA','S75892A','S2224XB','S75899A','S2231XA','S75901A','S2231XB','S75902A','S2232XA','S75909A','S2232XB','S75911A','S2239XA','S75912A','S2239XB','S75919A','S2241XA','S75991A','S2241XB','S75992A','S2242XA','S75999A','S2242XB','S76021A','S2243XA','S76022A','S2243XB','S76029A','S2249XA','S76121A','S2249XB','S76122A','S225XXA','S76129A','S225XXB','S76221A','S229XXA','S76222A','S229XXB','S76229A','S230XXA','S76321A','S23100A','S76322A','S23101A','S76329A','S23110A','S76821A','S23111A','S76822A','S23120A','S76829A','S23121A','S76921A','S23122A','S76922A','S23123A','S76929A','S23130A','S7700XA','S23131A','S7701XA','S23132A','S7702XA','S23133A','S7710XA','S23140A','S7711XA','S23141A','S7712XA','S23142A','S7720XA','S23143A','S7721XA','S23150A','S7722XA','S23151A','S78011A','S23152A','S78012A','S23153A','S78019A','S23160A','S78021A','S23161A','S78022A','S23162A','S78029A','S23163A','S78111A','S23170A','S78112A','S23171A','S78119A','S2320XA','S78121A','S2329XA','S78122A','S240XXA','S78129A','S24101A','S78911A','S24102A','S78912A','S24103A','S78919A','S24104A','S78921A','S24109A','S78922A','S24111A','S78929A','S24112A','S79001A','S24113A','S79002A','S24114A','S79009A','S24119A','S79011A','S24131A','S79012A','S24132A','S79019A','S24133A','S79091A','S24134A','S79092A','S24139A','S79099A','S24141A','S79101A','S24142A','S79102A','S24143A','S79109A','S24144A','S79111A','S24149A','S79112A','S24151A','S79119A','S24152A','S79121A','S24153A','S79122A','S24154A','S79129A','S24159A','S79131A','S242XXA','S79132A','S2500XA','S79139A','S2501XA','S79141A','S2502XA','S79142A','S2509XA','S79149A','S25101A','S79191A','S25102A','S79192A','S25109A','S79199A','S25111A','S81001A','S25112A','S81002A','S25119A','S81009A','S25121A','S81011A','S25122A','S81012A','S25129A','S81019A','S25191A','S81021A','S25192A','S81022A','S25199A','S81029A','S2520XA','S81031A','S2521XA','S81032A','S2522XA','S81039A','S2529XA','S81041A','S25301A','S81042A','S25302A','S81049A','S25309A','S81051A','S25311A','S81052A','S25312A','S81059A','S25319A','S81801A','S25321A','S81802A','S25322A','S81809A','S25329A','S81811A','S25391A','S81812A','S25392A','S81819A','S25399A','S81821A','S25401A','S81822A','S25402A','S81829A','S25409A','S81831A','S25411A','S81832A','S25412A','S81839A','S25419A','S81841A','S25421A','S81842A','S25422A','S81849A','S25429A','S81851A','S25491A','S81852A','S25492A','S81859A','S25499A','S82001A','S25501A','S82001B','S25502A','S82001C','S25509A','S82002A','S25511A','S82002B','S25512A','S82002C','S25519A','S82009A','S25591A','S82009B','S25592A','S82009C','S25599A','S82011A','S25801A','S82011B','S25802A','S82011C','S25809A','S82012A','S25811A','S82012B','S25812A','S82012C','S25819A','S82013A','S25891A','S82013B','S25892A','S82013C','S25899A','S82014A','S2590XA','S82014B','S2591XA','S82014C','S2599XA','S82015A','S2600XA','S82015B','S2601XA','S82015C','S26020A','S82016A','S26021A','S82016B','S26022A','S82016C','S2609XA','S82021A','S2610XA','S82021B','S2611XA','S82021C','S2612XA','S82022A','S2619XA','S82022B','S2690XA','S82022C','S2691XA','S82023A','S2692XA','S82023B','S2699XA','S82023C','S270XXA','S82024A','S271XXA','S272XXA','S82024C','S27301A','S82025A','S27302A','S82025B','S27309A','S82025C','S27311A','S82026A','S27312A','S82026B','S27319A','S82026C','S27321A','S82031A','S27322A','S82031B','S27329A','S82031C','S27331A','S82032A','S27332A','S82032B','S27339A','S82032C','S27391A','S82033A','S27392A','S82033B','S27399A','S82033C','S27401A','S82034A','S27402A','S82034B','S27409A','S82034C','S27411A','S82035A','S27412A','S82035B','S27419A','S82035C','S27421A','S82036A','S27422A','S82036B','S27429A','S82036C','S27431A','S82041A','S27432A','S82041B','S27439A','S82041C','S27491A','S82042A','S27492A','S82042B','S27499A','S82042C','S2750XA','S82043A','S2751XA','S82043B','S2752XA','S82043C','S2753XA','S82044A','S2759XA','S82044B','S2760XA','S82044C','S2763XA','S2769XA','S27802A','S27803A','S27808A','S82046B','S27809A','S82046C','S27812A','S82091A','S27813A','S82091B','S27818A','S82091C','S27819A','S82092A','S27892A','S82092B','S27893A','S82092C','S27898A','S82099A','S27899A','S82099B','S279XXA','S82099C','S280XXA','S281XXA','S82101B','S28211A','S82101C','S28212A','S82102A','S28219A','S82102B','S28221A','S82102C','S28222A','S82109A','S28229A','S82109B','S29021A','S82109C','S29022A','S82111A','S29029A','S82111B','S31000A','S82111C','S31001A','S82112A','S31010A','S82112B','S31011A','S82112C','S31020A','S82113A','S31021A','S82113B','S31030A','S82113C','S31031A','S82114A','S31040A','S82114B','S31041A','S82114C','S31050A','S82115A','S31051A','S82115B','S31100A','S82115C','S31101A','S82116A','S31102A','S82116B','S31103A','S82116C','S31104A','S82121A','S31105A','S82121B','S31109A','S82121C','S31110A','S82122A','S31111A','S82122B','S31112A','S82122C','S31113A','S82123A','S31114A','S82123B','S31115A','S82123C','S31119A','S82124A','S31120A','S82124B','S31121A','S82124C','S31122A','S82125A','S31123A','S82125B','S31124A','S82125C','S31125A','S82126A','S31129A','S82126B','S31130A','S82126C','S31131A','S82131A','S31132A','S82131B','S31133A','S82131C','S31134A','S82132A','S31135A','S82132B','S31139A','S82132C','S31140A','S82133A','S31141A','S82133B','S31142A','S82133C','S31143A','S82134A','S31144A','S82134B','S31145A','S82134C','S31149A','S82135A','S31150A','S82135B','S31151A','S82135C','S31152A','S82136A','S31153A','S82136B','S31154A','S82136C','S31155A','S82141A','S31159A','S82141B','S3120XA','S82141C','S3121XA','S82142A','S3122XA','S82142B','S3123XA','S82142C','S3124XA','S82143A','S3125XA','S3130XA','S82143C','S3131XA','S82144A','S3132XA','S82144B','S3133XA','S82144C','S3134XA','S82145A','S3135XA','S82145B','S3140XA','S82145C','S3141XA','S82146A','S3142XA','S82146B','S3143XA','S82146C','S3144XA','S82151A','S3145XA','S82151B','S31501A','S82151C','S31502A','S82152A','S31511A','S82152B','S31512A','S82152C','S31521A','S82153A','S31522A','S82153B','S31531A','S82153C','S31532A','S82154A','S31541A','S82154B','S31542A','S82154C','S31551A','S82155A','S31552A','S82155B','S31600A','S82155C','S31601A','S82156A','S31602A','S82156B','S31603A','S82156C','S31604A','S82161A','S31605A','S82162A','S31609A','S82169A','S31610A','S82191A','S31611A','S82191B','S31612A','S82191C','S31613A','S82192A','S31614A','S82192B','S31615A','S82192C','S31619A','S82199A','S31620A','S82199B','S31621A','S82199C','S31622A','S82201A','S31623A','S82201B','S31624A','S82201C','S31625A','S82202A','S31629A','S82202B','S31630A','S82202C','S31631A','S82209A','S31632A','S82209B','S31633A','S82209C','S31634A','S82221A','S31635A','S82221B','S31639A','S82221C','S31640A','S82222A','S31641A','S82222B','S31642A','S82222C','S31643A','S82223A','S31644A','S82223B','S31645A','S82223C','S31649A','S82224A','S31650A','S82224B','S31651A','S82224C','S31652A','S82225A','S31653A','S82225B','S31654A','S82225C','S31655A','S82226A','S31659A','S82226B','S31801A','S82226C','S31802A','S82231A','S31803A','S82231B','S31804A','S82231C','S31805A','S82232A','S31809A','S82232B','S31811A','S82232C','S31812A','S82233A','S31813A','S82233B','S31814A','S82233C','S31815A','S82234A','S31819A','S82234B','S31821A','S82234C','S31822A','S82235A','S31823A','S82235B','S31824A','S82235C','S31825A','S31829A','S82236B','S31831A','S82236C','S31832A','S82241A','S31833A','S82241B','S31834A','S82241C','S31835A','S31839A','S82242B','S32000A','S82242C','S32000B','S82243A','S32001A','S82243B','S32001B','S82243C','S32002A','S82244A','S32002B','S82244B','S32008A','S82244C','S32008B','S82245A','S32009A','S82245B','S32009B','S82245C','S32010A','S82246A','S32010B','S82246B','S32011A','S82246C','S32011B','S82251A','S32012A','S82251B','S32012B','S82251C','S32018A','S82252A','S32018B','S82252B','S32019A','S82252C','S32019B','S82253A','S32020A','S82253B','S32020B','S82253C','S32021A','S82254A','S32021B','S82254B','S32022A','S82254C','S32022B','S82255A','S32028A','S82255B','S32028B','S82255C','S32029A','S82256A','S32029B','S82256B','S32030A','S82256C','S32030B','S82261A','S32031A','S82261B','S32031B','S82261C','S32032A','S82262A','S32032B','S82262B','S32038A','S82262C','S32038B','S82263A','S32039A','S82263B','S32039B','S82263C','S32040A','S82264A','S32040B','S82264B','S32041A','S82264C','S32041B','S82265A','S32042A','S82265B','S32042B','S82265C','S32048A','S82266A','S32048B','S82266B','S32049A','S82266C','S32049B','S82291A','S32050A','S82291B','S32050B','S82291C','S32051A','S82292A','S32051B','S82292B','S32052A','S82292C','S32052B','S82299A','S32058A','S82299B','S32058B','S82299C','S32059A','S82301A','S32059B','S82301B','S3210XA','S82301C','S3210XB','S82302A','S32110A','S82302B','S32110B','S82302C','S32111A','S82309A','S32111B','S82309B','S32112A','S82309C','S32112B','S82311A','S32119A','S82312A','S32119B','S82319A','S32120A','S82391A','S32120B','S82391B','S32121A','S82391C','S32121B','S82392A','S32122A','S82392B','S32122B','S82392C','S32129A','S82399A','S32129B','S82399B','S32130A','S82399C','S32130B','S82401A','S32131A','S82401B','S32131B','S82401C','S32132A','S82402A','S32132B','S82402B','S32139A','S82402C','S32139B','S82409A','S3214XA','S82409B','S3214XB','S82409C','S3215XA','S82421A','S3215XB','S82421B','S3216XA','S82421C','S3216XB','S82422A','S3217XA','S82422B','S3217XB','S82422C','S3219XA','S82423A','S3219XB','S82423B','S322XXA','S82423C','S322XXB','S82424A','S32301A','S82424B','S32301B','S82424C','S32302A','S82425A','S32302B','S82425B','S32309A','S82425C','S32309B','S82426A','S32311A','S82426B','S32311B','S82426C','S32312A','S82431A','S32312B','S82431B','S32313A','S82431C','S32313B','S82432A','S32314A','S82432B','S32314B','S82432C','S32315A','S82433A','S32315B','S82433B','S32316A','S82433C','S32316B','S82434A','S32391A','S82434B','S32391B','S82434C','S32392A','S82435A','S32392B','S82435B','S32399A','S82435C','S32399B','S82436A','S32401A','S82436B','S32401B','S82436C','S32402A','S82441A','S32402B','S82441B','S32409A','S82441C','S32409B','S82442A','S32411A','S82442B','S32411B','S82442C','S32412A','S82443A','S32412B','S82443B','S32413A','S82443C','S32413B','S82444A','S32414A','S82444B','S32414B','S82444C','S32415A','S82445A','S32415B','S82445B','S32416A','S82445C','S32416B','S82446A','S32421A','S82446B','S32421B','S82446C','S32422A','S82451A','S32422B','S82451B','S32423A','S82451C','S32423B','S82452A','S32424A','S82452B','S32424B','S82452C','S32425A','S82453A','S32425B','S82453B','S32426A','S82453C','S32426B','S82454A','S32431A','S82454B','S32431B','S82454C','S32432A','S82455A','S32432B','S82455B','S32433A','S82455C','S32433B','S82456A','S32434A','S82456B','S32434B','S82456C','S32435A','S82461A','S32435B','S82461B','S32436A','S82461C','S32436B','S82462A','S32441A','S82462B','S32441B','S82462C','S32442A','S82463A','S32442B','S82463B','S32443A','S82463C','S32443B','S82464A','S32444A','S82464B','S32444B','S82464C','S32445A','S82465A','S32445B','S82465B','S32446A','S82465C','S32446B','S82466A','S32451A','S82466B','S32451B','S82466C','S32452A','S82491A','S32452B','S82491B','S32453A','S82491C','S32453B','S82492A','S32454A','S82492B','S32454B','S82492C','S32455A','S82499A','S32455B','S82499B','S32456A','S82499C','S32456B','S8251XA','S32461A','S8251XB','S32461B','S8251XC','S32462A','S8252XA','S32462B','S8252XB','S32463A','S8252XC','S32463B','S8253XA','S32464A','S8253XB','S32464B','S8253XC','S32465A','S8254XA','S32465B','S8254XB','S32466A','S8254XC','S32466B','S8255XA','S32471A','S8255XB','S32471B','S8255XC','S32472A','S8256XA','S32472B','S8256XB','S32473A','S8256XC','S32473B','S8261XA','S32474A','S8261XB','S32474B','S8261XC','S32475A','S8262XA','S32475B','S8262XB','S32476A','S8262XC','S32476B','S8263XA','S32481A','S8263XB','S32481B','S8263XC','S32482A','S8264XA','S32482B','S8264XB','S32483A','S8264XC','S32483B','S8265XA','S32484A','S8265XB','S32484B','S8265XC','S32485A','S8266XA','S32485B','S8266XB','S32486A','S8266XC','S32486B','S82811A','S32491A','S82812A','S32491B','S82819A','S32492A','S82821A','S32492B','S82822A','S32499A','S82829A','S32499B','S82831A','S32501A','S82831B','S32501B','S82831C','S32502A','S82832A','S32502B','S82832B','S32509A','S82832C','S32509B','S82839A','S32511A','S82839B','S32511B','S82839C','S32512A','S82841A','S32512B','S82841B','S32519A','S82841C','S32519B','S82842A','S32591A','S82842B','S32591B','S82842C','S32592A','S82843A','S32592B','S82843B','S32599A','S82843C','S32599B','S82844A','S32601A','S82844B','S32601B','S82844C','S32602A','S82845A','S32602B','S82845B','S32609A','S82845C','S32609B','S82846A','S32611A','S82846B','S32611B','S82846C','S32612A','S82851A','S32612B','S82851B','S32613A','S82851C','S32613B','S82852A','S32614A','S82852B','S32614B','S82852C','S32615A','S82853A','S32615B','S82853B','S32616A','S82853C','S32616B','S82854A','S32691A','S82854B','S32691B','S82854C','S32692A','S82855A','S32692B','S82855B','S32699A','S82855C','S32699B','S82856A','S32810A','S82856B','S32810B','S82856C','S32811A','S82861A','S32811B','S82861B','S3282XA','S82861C','S3282XB','S82862A','S3289XA','S82862B','S3289XB','S82862C','S329XXA','S82863A','S329XXB','S82863B','S330XXA','S82863C','S33100A','S82864A','S33101A','S82864B','S33110A','S82864C','S33111A','S82865A','S33120A','S82865B','S33121A','S82865C','S33130A','S82866A','S33131A','S82866B','S33140A','S82866C','S33141A','S82871A','S332XXA','S82871B','S3330XA','S82871C','S3339XA','S82872A','S3401XA','S82872B','S3402XA','S82872C','S34101A','S82873A','S34102A','S82873B','S34103A','S82873C','S34104A','S82874A','S34105A','S82874B','S34109A','S82874C','S34111A','S82875A','S34112A','S82875B','S34113A','S82875C','S34114A','S82876A','S34115A','S82876B','S34119A','S82876C','S34121A','S82891A','S34122A','S82891B','S34123A','S82891C','S34124A','S82892A','S34125A','S82892B','S34129A','S82892C','S34131A','S82899A','S34132A','S82899B','S34139A','S82899C','S3421XA','S8290XA','S3422XA','S8290XB','S343XXA','S344XXA','S8291XA','S3500XA','S8291XB','S3501XA','S8291XC','S3502XA','S8292XA','S3509XA','S8292XB','S3510XA','S8292XC','S3511XA','S83001A','S3512XA','S83002A','S3519XA','S83003A','S35211A','S83004A','S35212A','S83005A','S35218A','S83006A','S35219A','S83011A','S35221A','S83012A','S35222A','S83013A','S35228A','S83014A','S35229A','S83015A','S35231A','S83016A','S35232A','S83091A','S35238A','S83092A','S35239A','S83093A','S35291A','S83094A','S35292A','S83095A','S35298A','S83096A','S35299A','S83101A','S35311A','S35318A','S83103A','S35319A','S83104A','S35321A','S83105A','S35328A','S83106A','S35329A','S83111A','S35331A','S83112A','S35338A','S83113A','S35339A','S83114A','S35341A','S83115A','S35348A','S83116A','S35349A','S83121A','S35401A','S83122A','S35402A','S83123A','S35403A','S83124A','S35404A','S83125A','S35405A','S83126A','S35406A','S83131A','S35411A','S83132A','S35412A','S83133A','S35413A','S83134A','S35414A','S83135A','S35415A','S83136A','S35416A','S83141A','S35491A','S83142A','S35492A','S83143A','S35493A','S83144A','S35494A','S83145A','S35495A','S83146A','S35496A','S83191A','S3550XA','S83192A','S35511A','S83193A','S35512A','S35513A','S83195A','S35514A','S35515A','S35516A','S83201A','S35531A','S83202A','S35532A','S83203A','S35533A','S83204A','S35534A','S83205A','S35535A','S83206A','S35536A','S83207A','S3559XA','S83209A','S358X1A','S83211A','S358X8A','S83212A','S358X9A','S83219A','S3590XA','S83221A','S3591XA','S83222A','S3599XA','S83229A','S3600XA','S83231A','S36020A','S83232A','S36021A','S83239A','S36029A','S83241A','S36030A','S83242A','S36031A','S83249A','S36032A','S83251A','S36039A','S83252A','S3609XA','S36112A','S36113A','S83262A','S36114A','S36115A','S83271A','S36116A','S36118A','S36119A','S83281A','S36122A','S36123A','S83289A','S36128A','S8330XA','S36129A','S8331XA','S3613XA','S36200A','S85001A','S36201A','S85002A','S36202A','S85009A','S36209A','S85011A','S36220A','S85012A','S36221A','S85019A','S36222A','S85091A','S36229A','S85092A','S36230A','S85099A','S36231A','S85101A','S36232A','S85102A','S36239A','S85109A','S36240A','S85111A','S36241A','S85112A','S36242A','S85119A','S36249A','S85121A','S36250A','S85122A','S36251A','S85129A','S36252A','S85131A','S36259A','S85132A','S36260A','S85139A','S36261A','S85141A','S36262A','S85142A','S36269A','S85149A','S36290A','S85151A','S36291A','S85152A','S36292A','S85159A','S36299A','S85161A','S3630XA','S85162A','S3632XA','S3633XA','S3639XA','S36400A','S85179A','S36408A','S85181A','S36409A','S85182A','S36410A','S85189A','S36418A','S85201A','S36419A','S85202A','S36420A','S36428A','S85211A','S36429A','S85212A','S36430A','S36438A','S85291A','S36439A','S85292A','S36490A','S85299A','S36498A','S85301A','S36499A','S85302A','S36500A','S85309A','S36501A','S85311A','S36502A','S85312A','S36503A','S85319A','S36508A','S85391A','S36509A','S85392A','S36510A','S85399A','S36511A','S85401A','S36512A','S85402A','S36513A','S85409A','S36518A','S85411A','S36519A','S85412A','S36520A','S85419A','S36521A','S85491A','S36522A','S85492A','S36523A','S85499A','S36528A','S85501A','S36529A','S85502A','S36530A','S85509A','S36531A','S85511A','S36532A','S85512A','S36533A','S85519A','S36538A','S85591A','S36539A','S85592A','S36590A','S85599A','S36591A','S85801A','S36592A','S85802A','S36593A','S85809A','S36598A','S85811A','S36599A','S85812A','S3660XA','S85819A','S3661XA','S85891A','S3662XA','S3663XA','S3669XA','S3681XA','S36892A','S85909A','S36893A','S85911A','S36898A','S85912A','S36899A','S85919A','S3690XA','S85991A','S3692XA','S85992A','S3693XA','S85999A','S3699XA','S86021A','S37001A','S86022A','S37002A','S86029A','S37009A','S86121A','S37011A','S86122A','S37012A','S86129A','S37019A','S86221A','S37021A','S86222A','S37022A','S86229A','S37029A','S86321A','S37031A','S86322A','S37032A','S86329A','S37039A','S86821A','S37041A','S86822A','S37042A','S86829A','S37049A','S86921A','S37051A','S86922A','S37052A','S86929A','S37059A','S8700XA','S37061A','S8701XA','S37062A','S8702XA','S37069A','S8780XA','S37091A','S8781XA','S37092A','S8782XA','S37099A','S88011A','S3710XA','S88012A','S3712XA','S3713XA','S3719XA','S3720XA','S88029A','S3722XA','S3723XA','S3729XA','S3730XA','S88121A','S3732XA','S3733XA','S3739XA','S37401A','S88912A','S37402A','S88919A','S37409A','S88921A','S37421A','S88922A','S37422A','S88929A','S37429A','S89001A','S37431A','S89002A','S37432A','S89009A','S37439A','S89011A','S37491A','S89012A','S37492A','S89019A','S37499A','S89021A','S37501A','S89022A','S37502A','S89029A','S37509A','S89031A','S37511A','S89032A','S37512A','S89039A','S37519A','S89041A','S37521A','S89042A','S37522A','S89049A','S37529A','S89091A','S37531A','S89092A','S37532A','S89099A','S37539A','S89101A','S37591A','S89102A','S37592A','S89109A','S37599A','S89111A','S3760XA','S89112A','S3762XA','S3763XA','S3769XA','S37812A','S89129A','S37813A','S89131A','S37818A','S89132A','S37819A','S89139A','S37822A','S37823A','S37828A','S37829A','S89191A','S37892A','S89192A','S37893A','S89199A','S37898A','S89201A','S37899A','S89202A','S3790XA','S89209A','S3792XA','S89211A','S3793XA','S89212A','S3799XA','S89219A','S38001A','S89221A','S38002A','S89222A','S3801XA','S3802XA','S89291A','S3803XA','S381XXA','S89299A','S38211A','S89301A','S38212A','S89302A','S38221A','S89309A','S38222A','S89311A','S38231A','S89312A','S38232A','S89319A','S383XXA','S89321A','S39021A','S89322A','S39022A','S89329A','S39023A','S89391A','S41001A','S89392A','S41002A','S89399A','S41009A','S91001A','S41011A','S91002A','S41012A','S91009A','S41019A','S91011A','S41021A','S91012A','S41022A','S91019A','S41029A','S91021A','S41031A','S91022A','S41032A','S91029A','S41039A','S91031A','S41041A','S91032A','S41042A','S91039A','S41049A','S91041A','S41051A','S91042A','S41052A','S91049A','S41059A','S91051A','S41101A','S91052A','S41102A','S91059A','S41109A','S91109A','S41111A','S91301A','S41112A','S91302A','S41119A','S91309A','S41121A','S91311A','S41122A','S91312A','S41129A','S91319A','S41131A','S91321A','S41132A','S91322A','S41139A','S91329A','S41141A','S91331A','S41142A','S91332A','S41149A','S91339A','S41151A','S91341A','S41152A','S91342A','S41159A','S91349A','S42001A','S91351A','S42001B','S91352A','S42002A','S91359A','S42002B','S92001A','S42009A','S92001B','S42009B','S92002A','S42011A','S92002B','S42011B','S92009A','S42012A','S92009B','S42012B','S92011A','S42013A','S92011B','S42013B','S92012A','S42014A','S92012B','S42014B','S92013A','S42015A','S92013B','S42015B','S92014A','S42016A','S92014B','S42016B','S92015A','S42017A','S92015B','S42017B','S92016A','S42018A','S92016B','S42018B','S92021A','S42019A','S92021B','S42019B','S92022A','S42021A','S92022B','S42021B','S92023A','S42022A','S92023B','S42022B','S92024A','S42023A','S92024B','S42023B','S92025A','S42024A','S92025B','S42024B','S92026A','S42025A','S92026B','S42025B','S92031A','S42026A','S92031B','S42026B','S92032A','S42031A','S92032B','S42031B','S92033A','S42032A','S92033B','S42032B','S92034A','S42033A','S92034B','S42033B','S92035A','S42034A','S92035B','S42034B','S92036A','S42035A','S92036B','S42035B','S92041A','S42036A','S92041B','S42036B','S92042A','S42101A','S92042B','S42101B','S92043A','S42102A','S92043B','S42102B','S92044A','S42109A','S92044B','S42109B','S92045A','S42111A','S92045B','S42111B','S92046A','S42112A','S92046B','S42112B','S92051A','S42113A','S92051B','S42113B','S92052A','S42114A','S92052B','S42114B','S92053A','S42115A','S92053B','S42115B','S92054A','S42116A','S92054B','S42116B','S92055A','S42121A','S92055B','S42121B','S92056A','S42122A','S92056B','S42122B','S92061A','S42123A','S92061B','S42123B','S92062A','S42124A','S92062B','S42124B','S92063A','S42125A','S92063B','S42125B','S92064A','S42126A','S92064B','S42126B','S92065A','S42131A','S92065B','S42131B','S92066A','S42132A','S92066B','S42132B','S92101A','S42133A','S92101B','S42133B','S92102A','S42134A','S92102B','S42134B','S92109A','S42135A','S92109B','S42135B','S92111A','S42136A','S92111B','S42136B','S92112A','S42141A','S92112B','S42141B','S92113A','S42142A','S92113B','S42142B','S92114A','S42143A','S92114B','S42143B','S92115A','S42144A','S92115B','S42144B','S92116A','S42145A','S92116B','S42145B','S92121A','S42146A','S92121B','S42146B','S92122A','S42151A','S92122B','S42151B','S92123A','S42152A','S92123B','S42152B','S92124A','S42153A','S92124B','S42153B','S92125A','S42154A','S92125B','S42154B','S92126A','S42155A','S92126B','S42155B','S92131A','S42156A','S92131B','S42156B','S92132A','S42191A','S92132B','S42191B','S92133A','S42192A','S92133B','S42192B','S92134A','S42199A','S92134B','S42199B','S92135A','S42201A','S92135B','S42201B','S92136A','S42202A','S92136B','S42202B','S92141A','S42209A','S92141B','S42209B','S92142A','S42211A','S92142B','S42211B','S92143A','S42212A','S92143B','S42212B','S92144A','S42213A','S92144B','S42213B','S92145A','S42214A','S92145B','S42214B','S92146A','S42215A','S92146B','S42215B','S92151A','S42216A','S92151B','S42216B','S92152A','S42221A','S92152B','S42221B','S92153A','S42222A','S92153B','S42222B','S92154A','S42223A','S92154B','S42223B','S92155A','S42224A','S92155B','S42224B','S92156A','S42225A','S92156B','S42225B','S92191A','S42226A','S92191B','S42226B','S92192A','S42231A','S92192B','S42231B','S92199A','S42232A','S92199B','S42232B','S92201A','S42239A','S92201B','S42239B','S92202A','S42241A','S92202B','S42241B','S92209A','S42242A','S92209B','S42242B','S92211A','S42249A','S92211B','S42249B','S92212A','S42251A','S92212B','S42251B','S92213A','S42252A','S92213B','S42252B','S92214A','S42253A','S92214B','S42253B','S92215A','S42254A','S92215B','S42254B','S92216A','S42255A','S92216B','S42255B','S92221A','S42256A','S92221B','S42256B','S92222A','S42261A','S92222B','S42261B','S92223A','S42262A','S92223B','S42262B','S92224A','S42263A','S92224B','S42263B','S92225A','S42264A','S92225B','S42264B','S92226A','S42265A','S92226B','S42265B','S92231A','S42266A','S92231B','S42266B','S92232A','S42271A','S92232B','S42272A','S92233A','S42279A','S92233B','S42291A','S92234A','S42291B','S92234B','S42292A','S92235A','S42292B','S92235B','S42293A','S92236A','S42293B','S92236B','S42294A','S92241A','S42294B','S92241B','S42295A','S92242A','S42295B','S92242B','S42296A','S92243A','S42296B','S92243B','S42301A','S92244A','S42301B','S92244B','S42302A','S92245A','S42302B','S92245B','S42309A','S92246A','S42309B','S92246B','S42311A','S92251A','S42312A','S92251B','S42319A','S92252A','S42321A','S92252B','S42321B','S92253A','S42322A','S92253B','S42322B','S92254A','S42323A','S92254B','S42323B','S92255A','S42324A','S92255B','S42324B','S92256A','S42325A','S92256B','S42325B','S92301A','S42326A','S92301B','S42326B','S92302A','S42331A','S92302B','S42331B','S92309A','S42332A','S92309B','S42332B','S92311A','S42333A','S92311B','S42333B','S92312A','S42334A','S92312B','S42334B','S92313A','S42335A','S92313B','S42335B','S92314A','S42336A','S92314B','S42336B','S92315A','S42341A','S92315B','S42341B','S92316A','S42342A','S92316B','S42342B','S92321A','S42343A','S92321B','S42343B','S92322A','S42344A','S92322B','S42344B','S92323A','S42345A','S92323B','S42345B','S92324A','S42346A','S92324B','S42346B','S92325A','S42351A','S92325B','S42351B','S92326A','S42352A','S92326B','S42352B','S92331A','S42353A','S92331B','S42353B','S92332A','S42354A','S92332B','S42354B','S92333A','S42355A','S92333B','S42355B','S92334A','S42356A','S92334B','S42356B','S92335A','S42361A','S92335B','S42361B','S92336A','S42362A','S92336B','S42362B','S92341A','S42363A','S92341B','S42363B','S92342A','S42364A','S92342B','S42364B','S92343A','S42365A','S92343B','S42365B','S92344A','S42366A','S92344B','S42366B','S92345A','S42391A','S92345B','S42391B','S92346A','S42392A','S92346B','S42392B','S92351A','S42399A','S92351B','S42399B','S92352A','S42401A','S92352B','S42401B','S92353A','S42402A','S92353B','S42402B','S92354A','S42409A','S92354B','S42409B','S92355A','S42411A','S92355B','S42411B','S92356A','S42412A','S92356B','S42412B','S92811A','S42413A','S92811B','S42413B','S92812A','S42414A','S92812B','S42414B','S92819A','S42415A','S92819B','S42415B','S92901A','S42416A','S92901B','S42416B','S92902A','S42421A','S92902B','S42421B','S92909A','S42422A','S92909B','S42422B','S9301XA','S42423A','S9302XA','S42423B','S9303XA','S42424A','S9304XA','S42424B','S9305XA','S42425A','S9306XA','S42425B','S93101A','S42426A','S93102A','S42426B','S93103A','S42431A','S93104A','S42431B','S93105A','S42432A','S93106A','S42432B','S93111A','S42433A','S93112A','S42433B','S93113A','S42434A','S93114A','S42434B','S93115A','S42435A','S93116A','S42435B','S93119A','S42436A','S93121A','S42436B','S93122A','S42441A','S93123A','S42441B','S93124A','S42442A','S93125A','S42442B','S93126A','S42443A','S93129A','S42443B','S93131A','S42444A','S93132A','S42444B','S93133A','S42445A','S93134A','S42445B','S93135A','S42446A','S93136A','S42446B','S93139A','S42447A','S93141A','S42447B','S93142A','S42448A','S93143A','S42448B','S93144A','S42449A','S93145A','S42449B','S93146A','S42451A','S93149A','S42451B','S93301A','S42452A','S93302A','S42452B','S93303A','S42453A','S93304A','S42453B','S93305A','S42454A','S93306A','S42454B','S93311A','S42455A','S93312A','S42455B','S93313A','S42456A','S93314A','S42456B','S93315A','S42461A','S93316A','S42461B','S93321A','S42462A','S93322A','S42462B','S93323A','S42463A','S93324A','S42463B','S93325A','S42464A','S93326A','S42464B','S93331A','S42465A','S93332A','S42465B','S93333A','S42466A','S93334A','S42466B','S93335A','S42471A','S93336A','S42471B','S95001A','S42472A','S95002A','S42472B','S95009A','S42473A','S95011A','S42473B','S95012A','S42474A','S95019A','S42474B','S95091A','S42475A','S95092A','S42475B','S95099A','S42476A','S95101A','S42476B','S95102A','S42481A','S95109A','S42482A','S95111A','S42489A','S95112A','S42491A','S95119A','S42491B','S95191A','S42492A','S95192A','S42492B','S95199A','S42493A','S95201A','S42493B','S95202A','S42494A','S95209A','S42494B','S95211A','S42495A','S95212A','S42495B','S95219A','S42496A','S95291A','S42496B','S95292A','S4290XA','S95299A','S4290XB','S95801A','S4291XA','S95802A','S4291XB','S95809A','S4292XA','S95811A','S4292XB','S95812A','S43001A','S95819A','S43002A','S95891A','S43003A','S95892A','S43004A','S95899A','S43005A','S95901A','S43006A','S95902A','S43011A','S95909A','S43012A','S95911A','S43013A','S95912A','S43014A','S95919A','S43015A','S95991A','S43016A','S95992A','S43021A','S95999A','S43022A','S96021A','S43023A','S96022A','S43024A','S96029A','S43025A','S96121A','S43026A','S96122A','S43031A','S96129A','S43032A','S96221A','S43033A','S96222A','S43034A','S96229A','S43035A','S96821A','S43036A','S96822A','S43081A','S96829A','S43082A','S96921A','S43083A','S96922A','S43084A','S96929A','S43085A','S9700XA','S43086A','S9701XA','S43101A','S9702XA','S43102A','S97101A','S43109A','S97102A','S43111A','S97109A','S43112A','S97111A','S43119A','S97112A','S43121A','S97119A','S43122A','S97121A','S43129A','S97122A','S43131A','S97129A','S43132A','S9780XA','S43139A','S9781XA','S43141A','S9782XA','S43142A','S98011A','S43149A','S98012A','S43151A','S98019A','S43152A','S98021A','S43159A','S98022A','S43201A','S98029A','S43202A','S98311A','S43203A','S98312A','S43204A','S98319A','S43205A','S98321A','S43206A','S98322A','S43211A','S98329A','S43212A','S98911A','S43213A','S98912A','S43214A','S98919A','S43215A','S98921A','S43216A','S98922A','S43221A','S98929A','S43222A','S99001A','S43223A','S99001B','S43224A','S99002A','S43225A','S99002B','S43226A','S99009A','S43301A','S99009B','S43302A','S99011A','S43303A','S99011B','S43304A','S99012A','S43305A','S99012B','S43306A','S99019A','S43311A','S99019B','S43312A','S99021A','S43313A','S99021B','S43314A','S99022A','S43315A','S99022B','S43316A','S99029A','S43391A','S99029B','S43392A','S99031A','S43393A','S99031B','S43394A','S99032A','S43395A','S99032B','S43396A','S99039A','S45001A','S99039B','S45002A','S99041A','S45009A','S99041B','S45011A','S99042A','S45012A','S99042B','S45019A','S99049A','S45091A','S99049B','S45092A','S99091A','S45099A','S99091B','S45101A','S99092A','S45102A','S99092B','S45109A','S99099A','S45111A','S99099B','S45112A','S99101A','S45119A','S99101B','S45191A','S99102A','S45192A','S99102B','S45199A','S99109A','S45201A','S99109B','S45202A','S99111A','S45209A','S99111B','S45211A','S99112A','S45212A','S99112B','S45219A','S99119A','S45291A','S99119B','S45292A','S99121A','S45299A','S99121B','S45301A','S99122A','S45302A','S99122B','S45309A','S99129A','S45311A','S99129B','S45312A','S99131A','S45319A','S99131B','S45391A','S99132A','S45392A','S99132B','S45399A','S99139A','S45801A','S99139B','S45802A','S99141A','S45809A','S99141B','S45811A','S99142A','S45812A','S99142B','S45819A','S99149A','S45891A','S99149B','S45892A','S99191A','S45899A','S99191B','S45901A','S99192A','S45902A','S99192B','S45909A','S99199A','S45911A','S99199B','S45912A','S99201A','S45919A','S99201B','S45991A','S99202A','S45992A','S99202B','S45999A','S99209A','S46021A','S99209B','S46022A','S99211A','S46029A','S99211B','S46121A','S99212A','S46122A','S99212B','S46129A','S99219A','S46221A','S99219B','S46222A','S99221A','S46229A','S99221B','S46321A','S99222A','S46322A','S99222B','S46329A','S99229A','S46821A','S99229B','S46822A','S99231A','S46829A','S99231B','S46921A','S99232A','S46922A','S99232B','S46929A','S99239A','S471XXA','S99239B','S472XXA','S99241A','S479XXA','S99241B','S48011A','S99242A','S48012A','S99242B','S48019A','S99249A','S48021A','S99249B','S48022A','S99291A','S48029A','S99291B','S48111A','S99292A','S48112A','S99292B','S48119A','S99299A','S48121A','S99299B','S48122A','T07','S48129A','T07XXXA','S48911A','T148','S48912A','T148XXA','S48919A','T1490','S48921A','T1490XA','S48922A','T1491XA','S48929A','T2000XA','S49001A','T20011A','S49002A','T20012A','S49009A','T20019A','S49011A','T2002XA','S49012A','T2003XA','S49019A','T2004XA','S49021A','T2005XA','S49022A','T2006XA','S49029A','T2007XA','S49031A','T2009XA','S49032A','T2010XA','S49039A','T20111A','S49041A','T20112A','S49042A','T20119A','S49049A','T2012XA','S49091A','T2013XA','S49092A','T2014XA','S49099A','T2015XA','S49101A','T2016XA','S49102A','T2017XA','S49109A','T2019XA','S49111A','T2020XA','S49112A','T20211A','S49119A','T20212A','S49121A','T20219A','S49122A','T2022XA','S49129A','T2023XA','S49131A','T2024XA','S49132A','T2025XA','S49139A','T2026XA','S49141A','T2027XA','S49142A','T2029XA','S49149A','T2030XA','S49191A','T20311A','S49192A','T20312A','S49199A','T20319A','S51001A','T2032XA','S51002A','T2033XA','S51009A','T2034XA','S51011A','T2035XA','S51012A','T2036XA','S51019A','T2037XA','S51021A','T2039XA','S51022A','T2040XA','S51029A','T20411A','S51031A','T20412A','S51032A','T20419A','S51039A','T2042XA','S51041A','T2043XA','S51042A','T2044XA','S51049A','T2045XA','S51051A','S51052A','S51059A','T2049XA','S51801A','T2050XA','S51802A','T20511A','S51809A','T20512A','S51811A','T20519A','S51812A','T2052XA','S51819A','T2053XA','S51821A','T2054XA','S51822A','T2055XA','S51829A','T2056XA','S51831A','T2057XA','S51832A','T2059XA','S51839A','T2060XA','S51841A','T20611A','S51842A','T20612A','S51849A','T20619A','S51851A','T2062XA','S51852A','S51859A','T2064XA','S52001A','T2065XA','S52001B','T2066XA','S52001C','T2067XA','S52002A','T2069XA','S52002B','T2070XA','S52002C','T20711A','S52009A','T20712A','S52009B','T20719A','S52009C','T2072XA','S52011A','T2073XA','S52012A','T2074XA','S52019A','T2075XA','S52021A','T2076XA','S52021B','T2077XA','S52021C','T2079XA','S52022A','T2100XA','S52022B','T2101XA','S52022C','T2102XA','S52023A','T2103XA','S52023B','T2104XA','S52023C','T2105XA','S52024A','T2106XA','S52024B','T2107XA','S52024C','T2109XA','S52025A','T2110XA','S52025B','T2111XA','S52025C','T2112XA','S52026A','T2113XA','S52026B','T2114XA','S52026C','T2115XA','S52031A','T2116XA','S52031B','T2117XA','S52031C','T2119XA','S52032A','T2120XA','S52032B','T2121XA','S52032C','T2122XA','S52033A','T2123XA','S52033B','T2124XA','S52033C','T2125XA','S52034A','T2126XA','S52034B','T2127XA','S52034C','T2129XA','S52035A','T2130XA','S52035B','T2131XA','S52035C','T2132XA','S52036A','T2133XA','S52036B','T2134XA','S52036C','T2135XA','S52041A','T2136XA','S52041B','T2137XA','S52041C','T2139XA','S52042A','T2140XA','S52042B','T2141XA','S52042C','T2142XA','S52043A','T2143XA','S52043B','T2144XA','S52043C','T2145XA','S52044A','T2146XA','S52044B','T2147XA','S52044C','T2149XA','S52045A','T2150XA','S52045B','T2151XA','S52045C','T2152XA','S52046A','T2153XA','S52046B','T2154XA','S52046C','T2155XA','S52091A','T2156XA','S52091B','T2157XA','S52091C','T2159XA','S52092A','T2160XA','S52092B','T2161XA','S52092C','T2162XA','S52099A','T2163XA','S52099B','T2164XA','S52099C','T2165XA','S52101A','T2166XA','S52101B','T2167XA','S52101C','T2169XA','S52102A','T2170XA','S52102B','T2171XA','S52102C','T2172XA','S52109A','T2173XA','S52109B','T2174XA','S52109C','T2175XA','S52111A','T2176XA','S52112A','T2177XA','S52119A','T2179XA','S52121A','T2200XA','S52121B','T22011A','S52121C','T22012A','S52122A','T22019A','S52122B','T22021A','S52122C','T22022A','S52123A','T22029A','S52123B','T22031A','S52123C','T22032A','S52124A','T22039A','S52124B','T22041A','S52124C','T22042A','S52125A','T22049A','S52125B','T22051A','S52125C','T22052A','S52126A','T22059A','S52126B','T22061A','S52126C','T22062A','S52131A','T22069A','S52131B','T22091A','S52131C','T22092A','S52132A','T22099A','S52132B','T2210XA','S52132C','T22111A','S52133A','T22112A','S52133B','T22119A','S52133C','T22121A','S52134A','T22122A','S52134B','T22129A','S52134C','T22131A','S52135A','T22132A','S52135B','T22139A','S52135C','T22141A','S52136A','T22142A','S52136B','T22149A','S52136C','T22151A','S52181A','T22152A','S52181B','T22159A','S52181C','T22161A','S52182A','T22162A','S52182B','T22169A','S52182C','T22191A','S52189A','T22192A','S52189B','T22199A','S52189C','T2220XA','S52201A','T22211A','S52201B','T22212A','S52201C','T22219A','S52202A','T22221A','S52202B','T22222A','S52202C','T22229A','S52209A','T22231A','S52209B','T22232A','S52209C','T22239A','S52211A','T22241A','S52212A','T22242A','S52219A','T22249A','S52221A','T22251A','S52221B','T22252A','S52221C','T22259A','S52222A','T22261A','S52222B','T22262A','S52222C','T22269A','S52223A','T22291A','S52223B','T22292A','S52223C','T22299A','S52224A','T2230XA','S52224B','T22311A','S52224C','T22312A','S52225A','T22319A','S52225B','T22321A','S52225C','T22322A','S52226A','T22329A','S52226B','T22331A','S52226C','T22332A','S52231A','T22339A','S52231B','T22341A','S52231C','T22342A','S52232A','T22349A','S52232B','T22351A','S52232C','T22352A','S52233A','T22359A','S52233B','T22361A','S52233C','T22362A','S52234A','T22369A','S52234B','T22391A','S52234C','T22392A','S52235A','T22399A','S52235B','T2240XA','S52235C','T22411A','S52236A','T22412A','S52236B','T22419A','S52236C','T22421A','S52241A','T22422A','S52241B','T22429A','S52241C','T22431A','S52242A','T22432A','S52242B','T22439A','S52242C','T22441A','S52243A','T22442A','S52243B','T22449A','S52243C','T22451A','S52244A','T22452A','S52244B','T22459A','S52244C','T22461A','S52245A','T22462A','S52245B','T22469A','S52245C','T22491A','S52246A','T22492A','S52246B','T22499A','S52246C','T2250XA','S52251A','T22511A','S52251B','T22512A','S52251C','T22519A','S52252A','T22521A','S52252B','T22522A','S52252C','T22529A','S52253A','T22531A','S52253B','T22532A','S52253C','T22539A','S52254A','T22541A','S52254B','T22542A','S52254C','T22549A','S52255A','T22551A','S52255B','T22552A','S52255C','T22559A','S52256A','T22561A','S52256B','T22562A','S52256C','T22569A','S52261A','T22591A','S52261B','T22592A','S52261C','T22599A','S52262A','T2260XA','S52262B','T22611A','S52262C','T22612A','S52263A','T22619A','S52263B','T22621A','S52263C','T22622A','S52264A','T22629A','S52264B','T22631A','S52264C','T22632A','S52265A','T22639A','S52265B','T22641A','S52265C','T22642A','S52266A','T22649A','S52266B','T22651A','S52266C','T22652A','S52271A','T22659A','S52271B','T22661A','S52271C','T22662A','S52272A','T22669A','S52272B','T22691A','S52272C','T22692A','S52279A','T22699A','S52279B','T2270XA','S52279C','T22711A','S52281A','T22712A','S52281B','T22719A','S52281C','T22721A','S52282A','T22722A','S52282B','T22729A','S52282C','T22731A','S52283A','T22732A','S52283B','T22739A','S52283C','T22741A','S52291A','T22742A','S52291B','T22749A','S52291C','T22751A','S52292A','T22752A','S52292B','T22759A','S52292C','T22761A','S52299A','T22762A','S52299B','T22769A','S52299C','T22791A','S52301A','T22792A','S52301B','T22799A','S52301C','T23001A','S52302A','T23002A','S52302B','T23009A','S52302C','T23011A','S52309A','T23012A','S52309B','T23019A','S52309C','T23021A','S52311A','T23022A','S52312A','T23029A','S52319A','T23031A','S52321A','T23032A','S52321B','T23039A','S52321C','T23041A','S52322A','T23042A','S52322B','T23049A','S52322C','T23051A','S52323A','T23052A','S52323B','T23059A','S52323C','T23061A','S52324A','T23062A','S52324B','T23069A','S52324C','T23071A','S52325A','T23072A','S52325B','T23079A','S52325C','T23091A','S52326A','T23092A','S52326B','T23099A','S52326C','T23101A','S52331A','T23102A','S52331B','T23109A','S52331C','T23111A','S52332A','T23112A','S52332B','T23119A','S52332C','T23121A','S52333A','T23122A','S52333B','T23129A','S52333C','T23131A','S52334A','T23132A','S52334B','T23139A','S52334C','T23141A','S52335A','T23142A','S52335B','T23149A','S52335C','T23151A','S52336A','T23152A','S52336B','T23159A','S52336C','T23161A','S52341A','T23162A','S52341B','T23169A','S52341C','T23171A','S52342A','T23172A','S52342B','T23179A','S52342C','T23191A','S52343A','T23192A','S52343B','T23199A','S52343C','T23201A','S52344A','T23202A','S52344B','T23209A','S52344C','T23211A','S52345A','T23212A','S52345B','T23219A','S52345C','T23221A','S52346A','T23222A','S52346B','T23229A','S52346C','T23231A','S52351A','T23232A','S52351B','T23239A','S52351C','T23241A','S52352A','T23242A','S52352B','T23249A','S52352C','T23251A','S52353A','T23252A','S52353B','T23259A','S52353C','T23261A','S52354A','T23262A','S52354B','T23269A','S52354C','T23271A','S52355A','T23272A','S52355B','T23279A','S52355C','T23291A','S52356A','T23292A','S52356B','T23299A','S52356C','T23301A','S52361A','T23302A','S52361B','T23309A','S52361C','T23311A','S52362A','T23312A','S52362B','T23319A','S52362C','T23321A','S52363A','T23322A','S52363B','T23329A','S52363C','T23331A','S52364A','T23332A','S52364B','T23339A','S52364C','T23341A','S52365A','T23342A','S52365B','T23349A','S52365C','T23351A','S52366A','T23352A','S52366B','T23359A','S52366C','T23361A','S52371A','T23362A','S52371B','T23369A','S52371C','T23371A','S52372A','T23372A','S52372B','T23379A','S52372C','T23391A','S52379A','T23392A','S52379B','T23399A','S52379C','T23401A','S52381A','T23402A','S52381B','T23409A','S52381C','T23411A','S52382A','T23412A','S52382B','T23419A','S52382C','T23421A','S52389A','T23422A','S52389B','T23429A','S52389C','T23431A','S52391A','T23432A','S52391B','T23439A','S52391C','T23441A','S52392A','T23442A','S52392B','T23449A','S52392C','T23451A','S52399A','T23452A','S52399B','T23459A','S52399C','T23461A','S52501A','T23462A','S52501B','T23469A','S52501C','T23471A','S52502A','T23472A','S52502B','T23479A','S52502C','T23491A','S52509A','T23492A','S52509B','T23499A','S52509C','T23501A','S52511A','T23502A','S52511B','T23509A','S52511C','T23511A','S52512A','T23512A','S52512B','T23519A','S52512C','T23521A','S52513A','T23522A','S52513B','T23529A','S52513C','T23531A','S52514A','T23532A','S52514B','T23539A','S52514C','T23541A','S52515A','T23542A','S52515B','T23549A','S52515C','T23551A','S52516A','T23552A','S52516B','T23559A','S52516C','T23561A','S52521A','T23562A','S52522A','T23569A','S52529A','T23571A','S52531A','T23572A','S52531B','T23579A','S52531C','T23591A','S52532A','T23592A','S52532B','T23599A','S52532C','T23601A','S52539A','T23602A','S52539B','T23609A','S52539C','T23611A','S52541A','T23612A','S52541B','T23619A','S52541C','T23621A','S52542A','T23622A','S52542B','T23629A','S52542C','T23631A','S52549A','T23632A','S52549B','T23639A','S52549C','T23641A','S52551A','T23642A','S52551B','T23649A','S52551C','T23651A','S52552A','T23652A','S52552B','T23659A','S52552C','T23661A','S52559A','T23662A','S52559B','T23669A','S52559C','T23671A','S52561A','T23672A','S52561B','T23679A','S52561C','T23691A','S52562A','T23692A','S52562B','T23699A','S52562C','T23701A','S52569A','T23702A','S52569B','T23709A','S52569C','T23711A','S52571A','T23712A','S52571B','T23719A','S52571C','T23721A','S52572A','T23722A','S52572B','T23729A','S52572C','T23731A','S52579A','T23732A','S52579B','T23739A','S52579C','T23741A','S52591A','T23742A','S52591B','T23749A','S52591C','T23751A','S52592A','T23752A','S52592B','T23759A','S52592C','T23761A','S52599A','T23762A','S52599B','T23769A','S52599C','T23771A','S52601A','T23772A','S52601B','T23779A','S52601C','T23791A','S52602A','T23792A','S52602B','T23799A','S52602C','T24001A','S52609A','T24002A','S52609B','T24009A','S52609C','T24011A','S52611A','T24012A','S52611B','T24019A','S52611C','T24021A','S52612A','T24022A','S52612B','T24029A','S52612C','T24031A','S52613A','T24032A','S52613B','T24039A','S52613C','T24091A','S52614A','T24092A','S52614B','T24099A','S52614C','T24101A','S52615A','T24102A','S52615B','T24109A','S52615C','T24111A','S52616A','T24112A','S52616B','T24119A','S52616C','T24121A','S52621A','T24122A','S52622A','T24129A','S52629A','T24131A','S52691A','T24132A','S52691B','T24139A','S52691C','T24191A','S52692A','T24192A','S52692B','T24199A','S52692C','T24201A','S52699A','T24202A','S52699B','T24209A','S52699C','T24211A','S5290XA','T24212A','S5290XB','T24219A','S5290XC','T24221A','S5291XA','T24222A','S5291XB','T24229A','S5291XC','T24231A','S5292XA','T24232A','S5292XB','T24239A','S5292XC','T24291A','S53001A','T24292A','S53002A','T24299A','S53003A','T24301A','S53004A','T24302A','S53005A','T24309A','S53006A','T24311A','S53011A','T24312A','S53012A','T24319A','S53013A','T24321A','S53014A','T24322A','S53015A','T24329A','S53016A','T24331A','S53021A','T24332A','S53022A','T24339A','S53023A','T24391A','S53024A','T24392A','S53025A','T24399A','S53026A','T24401A','S53091A','T24402A','S53092A','T24409A','S53093A','T24411A','S53094A','T24412A','S53095A','T24419A','S53096A','T24421A','S53101A','T24422A','S53102A','T24429A','S53103A','T24431A','S53104A','T24432A','S53105A','T24439A','S53106A','T24491A','S53111A','T24492A','S53112A','T24499A','S53113A','T24501A','S53114A','T24502A','S53115A','T24509A','S53116A','T24511A','S53121A','T24512A','S53122A','T24519A','S53123A','T24521A','S53124A','T24522A','S53125A','T24529A','S53126A','T24531A','S53131A','T24532A','S53132A','T24539A','S53133A','T24591A','S53134A','T24592A','S53135A','T24599A','S53136A','T24601A','S53141A','T24602A','S53142A','T24609A','S53143A','T24611A','S53144A','T24612A','S53145A','T24619A','S53146A','T24621A','S53191A','T24622A','S53192A','T24629A','S53193A','T24631A','S53194A','T24632A','S53195A','T24639A','S53196A','T24691A','S55001A','T24692A','S55002A','T24699A','S55009A','T24701A','S55011A','T24702A','S55012A','T24709A','S55019A','T24711A','S55091A','T24712A','S55092A','T24719A','S55099A','T24721A','S55101A','T24722A','S55102A','T24729A','S55109A','T24731A','S55111A','T24732A','S55112A','T24739A','S55119A','T24791A','S55191A','T24792A','S55192A','T24799A','S55199A','T25011A','S55201A','T25012A','S55202A','T25019A','S55209A','T25021A','S55211A','T25022A','S55212A','T25029A','S55219A','T25031A','S55291A','T25032A','S55292A','T25039A','S55299A','T25091A','S55801A','T25092A','S55802A','T25099A','S55809A','T25111A','S55811A','T25112A','S55812A','T25119A','S55819A','T25121A','S55891A','T25122A','S55892A','T25129A','S55899A','T25131A','S55901A','T25132A','S55902A','T25139A','S55909A','T25191A','S55911A','T25192A','S55912A','T25199A','S55919A','T25211A','S55991A','T25212A','S55992A','T25219A','S55999A','T25221A','S56021A','T25222A','S56022A','T25229A','S56029A','T25231A','S56121A','T25232A','S56122A','T25239A','S56123A','T25291A','S56124A','T25292A','S56125A','T25299A','S56126A','T25311A','S56127A','T25312A','S56128A','T25319A','S56129A','T25321A','S56221A','T25322A','S56222A','T25329A','S56229A','T25331A','S56321A','T25332A','S56322A','T25339A','S56329A','T25391A','S56421A','T25392A','S56422A','T25399A','S56423A','T25411A','S56424A','T25412A','S56425A','T25419A','S56426A','T25421A','S56427A','T25422A','S56428A','T25429A','S56429A','T25431A','S56521A','T25432A','S56522A','T25439A','S56529A','T25491A','S56821A','T25492A','S56822A','T25499A','S56829A','T25511A','S56921A','T25512A','S56922A','T25519A','S56929A','T25521A','S5700XA','T25522A','S5701XA','T25529A','S5702XA','T25531A','S5780XA','T25532A','S5781XA','T25539A','S5782XA','T25591A','S58011A','T25592A','S58012A','T25599A','S58019A','T25611A','S58021A','T25612A','S58022A','T25619A','S58029A','T25621A','S58111A','T25622A','S58112A','T25629A','S58119A','T25631A','S58121A','T25632A','S58122A','T25639A','S58129A','T25691A','S58911A','T25692A','S58912A','T25699A','S58919A','T25711A','S58921A','T25712A','S58922A','T25719A','S58929A','T25721A','S59001A','T25722A','S59002A','T25729A','S59009A','T25731A','S59011A','T25732A','S59012A','T25739A','S59019A','T25791A','S59021A','T25792A','S59022A','T25799A','S59029A','T2600XA','S59031A','T2601XA','S59032A','T2602XA','S59039A','T2610XA','S59041A','T2611XA','S59042A','T2612XA','S59049A','T2620XA','S59091A','T2621XA','S59092A','T2622XA','S59099A','T2630XA','S59101A','T2631XA','S59102A','T2632XA','S59109A','T2640XA','S59111A','T2641XA','S59112A','T2642XA','S59119A','T2650XA','S59121A','T2651XA','S59122A','T2652XA','S59129A','T2660XA','S59131A','T2661XA','S59132A','T2662XA','S59139A','T2670XA','S59141A','T2671XA','S59142A','T2672XA','S59149A','T2680XA','S59191A','T2681XA','S59192A','T2682XA','S59199A','T2690XA','S59201A','T2691XA','S59202A','T2692XA','S59209A','T270XXA','S59211A','T271XXA','S59212A','T272XXA','S59219A','T273XXA','S59221A','T274XXA','S59222A','T275XXA','S59229A','T276XXA','S59231A','T277XXA','S59232A','T280XXA','S59239A','T281XXA','S59241A','T282XXA','S59242A','T283XXA','S59249A','T2840XA','S59291A','T28411A','S59292A','T28412A','S59299A','T28419A','S61401A','T2849XA','S61402A','T285XXA','S61409A','T286XXA','S61411A','T287XXA','S61412A','T288XXA','S61419A','T2890XA','S61421A','T28911A','S61422A','T28912A','S61429A','T28919A','S61431A','T2899XA','S61432A','T300','S61439A','T304','S61441A','T310','S61442A','T3110','S61449A','T3111','S61451A','S61452A','S61459A','T3122','S61501A','T3130','S61502A','T3131','S61509A','T3132','S61511A','T3133','S61512A','T3140','S61519A','T3141','S61521A','T3142','S61522A','T3143','S61529A','T3144','S61531A','T3150','S61532A','T3151','S61539A','T3152','S61541A','T3153','S61542A','T3154','S61549A','T3155','S61551A','S61552A','S61559A','T3162','S62001A','T3163','S62001B','T3164','S62002A','T3165','S62002B','T3166','S62009A','T3170','S62009B','T3171','S62011A','T3172','S62011B','T3173','S62012A','T3174','S62012B','T3175','S62013A','T3176','S62013B','T3177','S62014A','T3180','S62014B','T3181','S62015A','T3182','S62015B','T3183','S62016A','T3184','S62016B','T3185','S62021A','T3186','S62021B','T3187','S62022A','T3188','S62022B','T3190','S62023A','T3191','S62023B','T3192','S62024A','T3193','S62024B','T3194','S62025A','T3195','S62025B','T3196','S62026A','T3197','S62026B','T3198','S62031A','T3199','S62031B','T320','S62032A','T3210','S62032B','T3211','S62033A','T3220','S62033B','T3221','S62034A','T3222','S62034B','T3230','S62035A','T3231','S62035B','T3232','S62036A','T3233','S62036B','T3240','S62101A','T3241','S62101B','T3242','S62102A','T3243','S62102B','T3244','S62109A','T3250','S62109B','T3251','S62111A','T3252','S62111B','T3253','S62112A','T3254','S62112B','T3255','S62113A','T3260','S62113B','T3261','S62114A','T3262','S62114B','T3263','S62115A','T3264','S62115B','T3265','S62116A','T3266','S62116B','T3270','S62121A','T3271','S62121B','T3272','S62122A','T3273','S62122B','T3274','S62123A','T3275','S62123B','T3276','S62124A','T3277','S62124B','T3280','S62125A','T3281','S62125B','T3282','S62126A','T3283','S62126B','T3284','S62131A','T3285','S62131B','T3286','S62132A','T3287','S62132B','T3288','S62133A','T3290','S62133B','T3291','S62134A','T3292','S62134B','T3293','S62135A','T3294','S62135B','T3295','S62136A','T3296','S62136B','T3297','S62141A','T3298','S62141B','T3299','S62142A','T790XXA','S62142B','T791XXA','S62143A','T792XXA','S62143B','T794XXA','S62144A','T795XXA','S62144B','T796XXA','S62145A','T797XXA','S62145B','T798XXA','S62146A','T799XXA','S62146B') then 1 
-- MAGIC   else 0
-- MAGIC   end as traumid,
-- MAGIC
-- MAGIC   case 
-- MAGIC   when b.icd_code in ('C000','C001','C002','C003','C004','C005','C006','C008','C009','C01','C020','C021','C022','C023','C024','C028','C029','C030','C031','C039','C040','C041','C048','C049','C050','C051','C052','C058','C059','C060','C061','C062','C0680','C0689','C069','C07','C080','C081','C089','C090','C091','C098','C099','C100','C101','C102','C103','C104','C108','C109','C110','C111','C112','C113','C118','C119','C12','C130','C131','C132','C138','C139','C140','C142','C148','C153','C154','C155','C158','C159','C160','C161','C162','C163','C164','C165','C166','C168','C169','C170','C171','C172','C173','C178','C179','C180','C181','C182','C183','C184','C185','C186','C187','C188','C189','C19','C20','C210','C211','C212','C218','C220','C221','C222','C223','C224','C227','C228','C229','C23','C240','C241','C248','C249','C250','C251','C252','C253','C254','C257','C258','C259','C260','C261','C269','C300','C301','C310','C311','C312','C313','C318','C319','C320','C321','C322','C323','C328','C329','C33','C3400','C3401','C3402','C3410','C3411','C3412','C342','C3430','C3431','C3432','C3480','C3481','C3482','C3490','C3491','C3492','C37','C380','C381','C382','C383','C384','C388','C390','C399','C4000','C4001','C4002','C4010','C4011','C4012','C4020','C4021','C4022','C4030','C4031','C4032','C4080','C4081','C4082','C4090','C4091','C4092','C410','C411','C412','C413','C414','C419','C430','C4310','C4311','C43111','C43112','C4312','C43121','C43122','C4320','C4321','C4322','C4330','C4331','C4339','C434','C4351','C4352','C4359','C4360','C4361','C4362','C4370','C4371','C4372','C438','C439','C450','C451','C452','C457','C459','C460','C461','C462','C463','C464','C4650','C4651','C4652','C467','C469','C470','C4710','C4711','C4712','C4720','C4721','C4722','C473','C474','C475','C476','C478','C479','C480','C481','C482','C488','C490','C4910','C4911','C4912','C4920','C4921','C4922','C493','C494','C495','C496','C498','C499','C50011','C50012','C50019','C50021','C50022','C50029','C50111','C50112','C50119','C50121','C50122','C50129','C50211','C50212','C50219','C50221','C50222','C50229','C50311','C50312','C50319','C50321','C50322','C50329','C50411','C50412','C50419','C50421','C50422','C50429','C50511','C50512','C50519','C50521','C50522','C50529','C50611','C50612','C50619','C50621','C50622','C50629','C50811','C50812','C50819','C50821','C50822','C50829','C50911','C50912','C50919','C50921','C50922','C50929','C510','C511','C512','C518','C519','C52','C530','C531','C538','C539','C540','C541','C542','C543','C548','C549','C55','C561','C562','C563','C569','C5700','C5701','C5702','C5710','C5711','C5712','C5720','C5721','C5722','C573','C574','C577','C578','C579','C58','C600','C601','C602','C608','C609','C61','C6200','C6201','C6202','C6210','C6211','C6212','C6290','C6291','C6292','C6300','C6301','C6302','C6310','C6311','C6312','C632','C637','C638','C639','C641','C642','C649','C651','C652','C659','C661','C662','C669','C670','C671','C672','C673','C674','C675','C676','C677','C678','C679','C680','C681','C688','C689','C6900','C6901','C6902','C6910','C6911','C6912','C6920','C6921','C6922','C6930','C6931','C6932','C6940','C6941','C6942','C6950','C6951','C6952','C6960','C6961','C6962','C6980','C6981','C6982','C6990','C6991','C6992','C700','C701','C709','C710','C711','C712','C713','C714','C715','C716','C717','C718','C719','C720','C721','C7220','C7221','C7222','C7230','C7231','C7232','C7240','C7241','C7242','C7250','C7259','C729','C73','C7400','C7401','C7402','C7410','C7411','C7412','C7490','C7491','C7492','C750','C751','C752','C753','C754','C755','C758','C759','C760','C761','C762','C763','C7640','C7641','C7642','C7650','C7651','C7652','C768','C770','C771','C772','C773','C774','C775','C778','C779','C7800','C7801','C7802','C781','C782','C7830','C7839','C784','C785','C786','C787','C7880','C7889','C7900','C7901','C7902','C7910','C7911','C7919','C792','C7931','C7932','C7940','C7949','C7951','C7952','C7960','C7961','C7962','C7963','C7970','C7971','C7972','C7981','C7982','C7989','C799','C7A00','C7A010','C7A011','C7A012','C7A019','C7A020','C7A021','C7A022','C7A023','C7A024','C7A025','C7A026','C7A029','C7A090','C7A091','C7A092','C7A093','C7A094','C7A095','C7A096','C7A098','C7A1','C7A8','C7B00','C7B01','C7B02','C7B03','C7B04','C7B09','C7B1','C7B8','C800','C801','C8100','C8101','C8102','C8103','C8104','C8105','C8106','C8107','C8108','C8109','C8110','C8111','C8112','C8113','C8114','C8115','C8116','C8117','C8118','C8119','C8120','C8121','C8122','C8123','C8124','C8125','C8126','C8127','C8128','C8129','C8130','C8131','C8132','C8133','C8134','C8135','C8136','C8137','C8138','C8139','C8140','C8141','C8142','C8143','C8144','C8145','C8146','C8147','C8148','C8149','C8170','C8171','C8172','C8173','C8174','C8175','C8176','C8177','C8178','C8179','C8190','C8191','C8192','C8193','C8194','C8195','C8196','C8197','C8198','C8199','C8200','C8201','C8202','C8203','C8204','C8205','C8206','C8207','C8208','C8209','C8210','C8211','C8212','C8213','C8214','C8215','C8216','C8217','C8218','C8219','C8220','C8221','C8222','C8223','C8224','C8225','C8226','C8227','C8228','C8229','C8230','C8231','C8232','C8233','C8234','C8235','C8236','C8237','C8238','C8239','C8240','C8241','C8242','C8243','C8244','C8245','C8246','C8247','C8248','C8249','C8250','C8251','C8252','C8253','C8254','C8255','C8256','C8257','C8258','C8259','C8260','C8261','C8262','C8263','C8264','C8265','C8266','C8267','C8268','C8269','C8280','C8281','C8282','C8283','C8284','C8285','C8286','C8287','C8288','C8289','C8290','C8291','C8292','C8293','C8294','C8295','C8296','C8297','C8298','C8299','C8300','C8301','C8302','C8303','C8304','C8305','C8306','C8307','C8308','C8309','C8310','C8311','C8312','C8313','C8314','C8315','C8316','C8317','C8318','C8319','C8330','C8331','C8332','C8333','C8334','C8335','C8336','C8337','C8338','C8339','C8350','C8351','C8352','C8353','C8354','C8355','C8356','C8357','C8358','C8359','C8370','C8371','C8372','C8373','C8374','C8375','C8376','C8377','C8378','C8379','C8380','C8381','C8382','C8383','C8384','C8385','C8386','C8387','C8388','C8389','C8390','C8391','C8392','C8393','C8394','C8395','C8396','C8397','C8398','C8399','C8400','C8401','C8402','C8403','C8404','C8405','C8406','C8407','C8408','C8409','C8410','C8411','C8412','C8413','C8414','C8415','C8416','C8417','C8418','C8419','C8440','C8441','C8442','C8443','C8444','C8445','C8446','C8447','C8448','C8449','C8460','C8461','C8462','C8463','C8464','C8465','C8466','C8467','C8468','C8469','C8470','C8471','C8472','C8473','C8474','C8475','C8476','C8477','C8478','C8479','C847A','C8490','C8491','C8492','C8493','C8494','C8495','C8496','C8497','C8498','C8499','C84A0','C84A1','C84A2','C84A3','C84A4','C84A5','C84A6','C84A7','C84A8','C84A9','C84Z0','C84Z1','C84Z2','C84Z3','C84Z4','C84Z5','C84Z6','C84Z7','C84Z8','C84Z9','C8510','C8511','C8512','C8513','C8514','C8515','C8516','C8517','C8518','C8519','C8520','C8521','C8522','C8523','C8524','C8525','C8526','C8527','C8528','C8529','C8580','C8581','C8582','C8583','C8584','C8585','C8586','C8587','C8588','C8589','C8590','C8591','C8592','C8593','C8594','C8595','C8596','C8597','C8598','C8599','C860','C861','C862','C863','C864','C865','C866','C880','C882','C883','C884','C888','C889','C9000','C9001','C9002','C9010','C9011','C9012','C9020','C9021','C9022','C9030','C9031','C9032','C9100','C9101','C9102','C9110','C9111','C9112','C9130','C9131','C9132','C9140','C9141','C9142','C9150','C9151','C9152','C9160','C9161','C9162','C9190','C9191','C9192','C91A0','C91A1','C91A2','C91Z0','C91Z1','C91Z2','C9200','C9201','C9202','C9210','C9211','C9212','C9220','C9221','C9222','C9230','C9231','C9232','C9240','C9241','C9242','C9250','C9251','C9252','C9260','C9261','C9262','C9290','C9291','C9292','C92A0','C92A1','C92A2','C92Z0','C92Z1','C92Z2','C9300','C9301','C9302','C9310','C9311','C9312','C9330','C9331','C9332','C9390','C9391','C9392','C93Z0','C93Z1','C93Z2','C9400','C9401','C9402','C9420','C9421','C9422','C9430','C9431','C9432','C9480','C9481','C9482','C9500','C9501','C9502','C9510','C9511','C9512','C9590','C9591','C9592','C960','C962','C9620','C9621','C9622','C9629','C964','C969','C96A','C96Z','D47Z2','D47Z9') then 1
-- MAGIC   else 0
-- MAGIC   end as canceid,
-- MAGIC
-- MAGIC   case 
-- MAGIC   when b.icd_code in ('A34','O000','O0000','O0001','O001','O0010','O00101','O00102','O00109','O0011','O00111','O00112','O00119','O002','O0020','O00201','O00202','O00209','O0021','O00211','O00212','O00219','O008','O0080','O0081','O009','O0090','O0091','O010','O011','O019','O020','O021','O0281','O0289','O029','O030','O031','O032','O0330','O0331','O0332','O0333','O0334','O0335','O0336','O0337','O0338','O0339','O034','O035','O036','O037','O0380','O0381','O0382','O0383','O0384','O0385','O0386','O0387','O0388','O0389','O039','O045','O046','O047','O0480','O0481','O0482','O0483','O0484','O0485','O0486','O0487','O0488','O0489','O070','O071','O072','O0730','O0731','O0732','O0733','O0734','O0735','O0736','O0737','O0738','O0739','O074','O080','O081','O082','O083','O084','O085','O086','O087','O0881','O0882','O0883','O0889','O089','O0900','O0901','O0902','O0903','O0910','O0911','O0912','O0913','O09211','O09212','O09213','O09219','O09291','O09292','O09293','O09299','O0930','O0931','O0932','O0933','O0940','O0941','O0942','O0943','O09511','O09512','O09513','O09519','O09521','O09522','O09523','O09529','O09611','O09612','O09613','O09619','O09621','O09622','O09623','O09629','O0970','O0971','O0972','O0973','O09811','O09812','O09813','O09819','O09821','O09822','O09823','O09829','O09891','O09892','O09893','O09899','O0990','O0991','O0992','O0993','O09A0','O09A1','O09A2','O09A3','O10011','O10012','O10013','O10019','O1002','O1003','O10111','O10112','O10113','O10119','O1012','O1013','O10211','O10212','O10213','O10219','O1022','O1023','O10311','O10312','O10313','O10319','O1032','O1033','O10411','O10412','O10413','O10419','O1042','O1043','O10911','O10912','O10913','O10919','O1092','O1093','O111','O112','O113','O114','O115','O119','O1200','O1201','O1202','O1203','O1204','O1205','O1210','O1211','O1212','O1213','O1214','O1215','O1220','O1221','O1222','O1223','O1224','O1225','O131','O132','O133','O134','O135','O139','O1400','O1402','O1403','O1404','O1405','O1410','O1412','O1413','O1414','O1415','O1420','O1422','O1423','O1424','O1425','O1490','O1492','O1493','O1494','O1495','O1500','O1502','O1503','O151','O152','O159','O161','O162','O163','O164','O165','O169','O200','O208','O209','O210','O211','O212','O218','O219','O2200','O2201','O2202','O2203','O2210','O2211','O2212','O2213','O2220','O2221','O2222','O2223','O2230','O2231','O2232','O2233','O2240','O2241','O2242','O2243','O2250','O2251','O2252','O2253','O228X1','O228X2','O228X3','O228X9','O2290','O2291','O2292','O2293','O2300','O2301','O2302','O2303','O2310','O2311','O2312','O2313','O2320','O2321','O2322','O2323','O2330','O2331','O2332','O2333','O2340','O2341','O2342','O2343','O23511','O23512','O23513','O23519','O23521','O23522','O23523','O23529','O23591','O23592','O23593','O23599','O2390','O2391','O2392','O2393','O24011','O24012','O24013','O24019','O2402','O2403','O24111','O24112','O24113','O24119','O2412','O2413','O24311','O24312','O24313','O24319','O2432','O2433','O24410','O24414','O24415','O24419','O24420','O24424','O24425','O24429','O24430','O24434','O24435','O24439','O24811','O24812','O24813','O24819','O2482','O2483','O24911','O24912','O24913','O24919','O2492','O2493','O2510','O2511','O2512','O2513','O252','O253','O2600','O2601','O2602','O2603','O2610','O2611','O2612','O2613','O2620','O2621','O2622','O2623','O2630','O2631','O2632','O2633','O2640','O2641','O2642','O2643','O2650','O2651','O2652','O2653','O26611','O26612','O26613','O26619','O2662','O2663','O26711','O26712','O26713','O26719','O2672','O2673','O26811','O26812','O26813','O26819','O26821','O26822','O26823','O26829','O26831','O26832','O26833','O26839','O26841','O26842','O26843','O26849','O26851','O26852','O26853','O26859','O2686','O26872','O26873','O26879','O26891','O26892','O26893','O26899','O2690','O2691','O2692','O2693','O280','O281','O282','O283','O284','O285','O288','O289','O29011','O29012','O29013','O29019','O29021','O29022','O29023','O29029','O29091','O29092','O29093','O29099','O29111','O29112','O29113','O29119','O29121','O29122','O29123','O29129','O29191','O29192','O29193','O29199','O29211','O29212','O29213','O29219','O29291','O29292','O29293','O29299','O293X1','O293X2','O293X3','O293X9','O2940','O2941','O2942','O2943','O295X1','O295X2','O295X3','O295X9','O2960','O2961','O2962','O2963','O298X1','O298X2','O298X3','O298X9','O2990','O2991','O2992','O2993','O30001','O30002','O30003','O30009','O30011','O30012','O30013','O30019','O30021','O30022','O30023','O30029','O30031','O30032','O30033','O30039','O30041','O30042','O30043','O30049','O30091','O30092','O30093','O30099','O30101','O30102','O30103','O30109','O30111','O30112','O30113','O30119','O30121','O30122','O30123','O30129','O30131','O30132','O30133','O30139','O30191','O30192','O30193','O30199','O30201','O30202','O30203','O30209','O30211','O30212','O30213','O30219','O30221','O30222','O30223','O30229','O30231','O30232','O30233','O30239','O30291','O30292','O30293','O30299','O30801','O30802','O30803','O30809','O30811','O30812','O30813','O30819','O30821','O30822','O30823','O30829','O30831','O30832','O30833','O30839','O30891','O30892','O30893','O30899','O3090','O3091','O3092','O3093','O3100X0','O3100X1','O3100X2','O3100X3','O3100X4','O3100X5','O3100X9','O3101X0','O3101X1','O3101X2','O3101X3','O3101X4','O3101X5','O3101X9','O3102X0','O3102X1','O3102X2','O3102X3','O3102X4','O3102X5','O3102X9','O3103X0','O3103X1','O3103X2','O3103X3','O3103X4','O3103X5','O3103X9','O3110X0','O3110X1','O3110X2','O3110X3','O3110X4','O3110X5','O3110X9','O3111X0','O3111X1','O3111X2','O3111X3','O3111X4','O3111X5','O3111X9','O3112X0','O3112X1','O3112X2','O3112X3','O3112X4','O3112X5','O3112X9','O3113X0','O3113X1','O3113X2','O3113X3','O3113X4','O3113X5','O3113X9','O3120X0','O3120X1','O3120X2','O3120X3','O3120X4','O3120X5','O3120X9','O3121X0','O3121X1','O3121X2','O3121X3','O3121X4','O3121X5','O3121X9','O3122X0','O3122X1','O3122X2','O3122X3','O3122X4','O3122X5','O3122X9','O3123X0','O3123X1','O3123X2','O3123X3','O3123X4','O3123X5','O3123X9','O3130X0','O3130X1','O3130X2','O3130X3','O3130X4','O3130X5','O3130X9','O3131X0','O3131X1','O3131X2','O3131X3','O3131X4','O3131X5','O3131X9','O3132X0','O3132X1','O3132X2','O3132X3','O3132X4','O3132X5','O3132X9','O3133X0','O3133X1','O3133X2','O3133X3','O3133X4','O3133X5','O3133X9','O318X10','O318X11','O318X12','O318X13','O318X14','O318X15','O318X19','O318X20','O318X21','O318X22','O318X23','O318X24','O318X25','O318X29','O318X30','O318X31','O318X32','O318X33','O318X34','O318X35','O318X39','O318X90','O318X91','O318X92','O318X93','O318X94','O318X95','O318X99','O320XX0','O320XX1','O320XX2','O320XX3','O320XX4','O320XX5','O320XX9','O321XX0','O321XX1','O321XX2','O321XX3','O321XX4','O321XX5','O321XX9','O322XX0','O322XX1','O322XX2','O322XX3','O322XX4','O322XX5','O322XX9','O323XX0','O323XX1','O323XX2','O323XX3','O323XX4','O323XX5','O323XX9','O324XX0','O324XX1','O324XX2','O324XX3','O324XX4','O324XX5','O324XX9','O326XX0','O326XX1','O326XX2','O326XX3','O326XX4','O326XX5','O326XX9','O328XX0','O328XX1','O328XX2','O328XX3','O328XX4','O328XX5','O328XX9','O329XX0','O329XX1','O329XX2','O329XX3','O329XX4','O329XX5','O329XX9','O330','O331','O332','O333XX0','O333XX1','O333XX2','O333XX3','O333XX4','O333XX5','O333XX9','O334XX0','O334XX1','O334XX2','O334XX3','O334XX4','O334XX5','O334XX9','O335XX0','O335XX1','O335XX2','O335XX3','O335XX4','O335XX5','O335XX9','O336XX0','O336XX1','O336XX2','O336XX3','O336XX4','O336XX5','O336XX9','O337','O337XX0','O337XX1','O337XX2','O337XX3','O337XX4','O337XX5','O337XX9','O338','O339','O3400','O3401','O3402','O3403','O3410','O3411','O3412','O3413','O3421','O34211','O34212','O34218','O34219','O3422','O3429','O3430','O3431','O3432','O3433','O3440','O3441','O3442','O3443','O34511','O34512','O34513','O34519','O34521','O34522','O34523','O34529','O34531','O34532','O34533','O34539','O34591','O34592','O34593','O34599','O3460','O3461','O3462','O3463','O3470','O3471','O3472','O3473','O3480','O3481','O3482','O3483','O3490','O3491','O3492','O3493','O3500X0','O3500X1','O3500X2','O3500X3','O3500X4','O3500X5','O3500X9','O3501X0','O3501X1','O3501X2','O3501X3','O3501X4','O3501X5','O3501X9','O3502X0','O3502X1','O3502X2','O3502X3','O3502X4','O3502X5','O3502X9','O3503X0','O3503X1','O3503X2','O3503X3','O3503X4','O3503X5','O3503X9','O3504X0','O3504X1','O3504X2','O3504X3','O3504X4','O3504X5','O3504X9','O3505X0','O3505X1','O3505X2','O3505X3','O3505X4','O3505X5','O3505X9','O3506X0','O3506X1','O3506X2','O3506X3','O3506X4','O3506X5','O3506X9','O3507X0','O3507X1','O3507X2','O3507X3','O3507X4','O3507X5','O3507X9','O3508X0','O3508X1','O3508X2','O3508X3','O3508X4','O3508X5','O3508X9','O3509X0','O3509X1','O3509X2','O3509X3','O3509X4','O3509X5','O3509X9','O350XX0','O350XX1','O350XX2','O350XX3','O350XX4','O350XX5','O350XX9','O3510X0','O3510X1','O3510X2','O3510X3','O3510X4','O3510X5','O3510X9','O3511X0','O3511X1','O3511X2','O3511X3','O3511X4','O3511X5','O3511X9','O3512X0','O3512X1','O3512X2','O3512X3','O3512X4','O3512X5','O3512X9','O3513X0','O3513X1','O3513X2','O3513X3','O3513X4','O3513X5','O3513X9','O3514X0','O3514X1','O3514X2','O3514X3','O3514X4','O3514X5','O3514X9','O3515X0','O3515X1','O3515X2','O3515X3','O3515X4','O3515X5','O3515X9','O3519X0','O3519X1','O3519X2','O3519X3','O3519X4','O3519X5','O3519X9','O351XX0','O351XX1','O351XX2','O351XX3','O351XX4','O351XX5','O351XX9','O352XX0','O352XX1','O352XX2','O352XX3','O352XX4','O352XX5','O352XX9','O353XX0','O353XX1','O353XX2','O353XX3','O353XX4','O353XX5','O353XX9','O354XX0','O354XX1','O354XX2','O354XX3','O354XX4','O354XX5','O354XX9','O355XX0','O355XX1','O355XX2','O355XX3','O355XX4','O355XX5','O355XX9','O356XX0','O356XX1','O356XX2','O356XX3','O356XX4','O356XX5','O356XX9','O357XX0','O357XX1','O357XX2','O357XX3','O357XX4','O357XX5','O357XX9','O358XX0','O358XX1','O358XX2','O358XX3','O358XX4','O358XX5','O358XX9','O359XX0','O359XX1','O359XX2','O359XX3','O359XX4','O359XX5','O359XX9','O35AXX0','O35AXX1','O35AXX2','O35AXX3','O35AXX4','O35AXX5','O35AXX9','O35BXX0','O35BXX1','O35BXX2','O35BXX3','O35BXX4','O35BXX5','O35BXX9','O35CXX0','O35CXX1','O35CXX2','O35CXX3','O35CXX4','O35CXX5','O35CXX9','O35DXX0','O35DXX1','O35DXX2','O35DXX3','O35DXX4','O35DXX5','O35DXX9','O35EXX0','O35EXX1','O35EXX2','O35EXX3','O35EXX4','O35EXX5','O35EXX9','O35FXX0','O35FXX1','O35FXX2','O35FXX3','O35FXX4','O35FXX5','O35FXX9','O35GXX0','O35GXX1','O35GXX2','O35GXX3','O35GXX4','O35GXX5','O35GXX9','O35HXX0','O35HXX1','O35HXX2','O35HXX3','O35HXX4','O35HXX5','O35HXX9','O360110','O360111','O360112','O360113','O360114','O360115','O360119','O360120','O360121','O360122','O360123','O360124','O360125','O360129','O360130','O360131','O360132','O360133','O360134','O360135','O360139','O360190','O360191','O360192','O360193','O360194','O360195','O360199','O360910','O360911','O360912','O360913','O360914','O360915','O360919','O360920','O360921','O360922','O360923','O360924','O360925','O360929','O360930','O360931','O360932','O360933','O360934','O360935','O360939','O360990','O360991','O360992','O360993','O360994','O360995','O360999','O361110','O361111','O361112','O361113','O361114','O361115','O361119','O361120','O361121','O361122','O361123','O361124','O361125','O361129','O361130','O361131','O361132','O361133','O361134','O361135','O361139','O361190','O361191','O361192','O361193','O361194','O361195','O361199','O361910','MDC 15 Principal diagnosis codes: (MDC15PRINDX)','A33','E8411','P000','P001','P003','P004','P005','P006','P007','P0081','P009','P010','P011','P012','P013','P014','P015','P016','P017','P018','P019','P020','P021','P0220','P0229','P023','P024','P025','P0260','P0269','P027','P0270','P0278','P028','P029','P030','P031','P032','P033','P034','P035','P036','P03810','P03811','P03819','P0382','P0389','P039','P040','P041','P0411','P0412','P0413','P0414','P0415','P0416','P0417','P0418','P0419','P041A','P042','P043','P0440','P0441','P0442','P0449','P045','P046','P048','P0481','P0489','P049','P0500','P0501','P0502','P0503','P0504','P0505','P0506','P0507','P0508','P0509','P0510','P0511','P0512','P0513','P0514','P0515','P0516','P0517','P0518','P0519','P052','P059','P0700','P0701','P0702','P0703','P0710','P0714','P0715','P0716','P0717','P0718','P0720','P0721','P0722','P0723','P0724','P0725','P0726','P0730','P0731','P0732','P0733','P0734','P0735','P0736','P0737','P0738','P0739','P080','P081','P0821','P0822','P100','P101','P102','P103','P104','P108','P109','P110','P111','P112','P113','P114','P115','P119','P120','P121','P122','P123','P124','P1281','P1289','P129','P130','P131','P132','P133','P134','P138','P139','P140','P141','P142','P143','P148','P149','P150','P151','P152','P153','P154','P155','P156','P158','P159','P190','P191','P192','P199','P220','P221','P228','P229','P230','P231','P232','P233','P234','P235','P236','P238','P239','P2400','P2401','P2410','P2411','P2420','P2421','P2430','P2431','P2480','P2481','P249','P250','P251','P252','P253','P258','P260','P261','P268','P269','P280','P2810','P2811','P2819','P282','P283','P2830','P2831','P2832','P2833','P2839','P284','P2840','P2841','P2842','P2843','P2849','P285','P2881','P2889','P289','P290','P2911','P2912','P292','P293','P2930','P2938','P294','P2981','P2989','P299','P350','P351','P352','O361911','O361912','O361913','O361914','O361915','O361919','O361920','O361921','O361922','O361923','O361924','O361925','O361929','O361930','O361931','O361932','O361933','O361934','O361935','O361939','O361990','O361991','O361992','O361993','O361994','O361995','O361999','O3620X0','O3620X1','O3620X2','O3620X3','O3620X4','O3620X5','O3620X9','O3621X0','O3621X1','O3621X2','O3621X3','O3621X4','O3621X5','O3621X9','O3622X0','O3622X1','O3622X2','O3622X3','O3622X4','O3622X5','O3622X9','O3623X0','O3623X1','O3623X2','O3623X3','O3623X4','O3623X5','O3623X9','O364XX0','O364XX1','O364XX2','O364XX3','O364XX4','O364XX5','O364XX9','O365110','O365111','O365112','O365113','O365114','O365115','O365119','O365120','O365121','O365122','O365123','O365124','O365125','O365129','O365130','O365131','O365132','O365133','O365134','O365135','O365139','O365190','O365191','O365192','O365193','O365194','O365195','O365199','O365910','O365911','O365912','O365913','O365914','O365915','O365919','O365920','O365921','O365922','O365923','O365924','O365925','O365929','O365930','O365931','O365932','O365933','O365934','O365935','O365939','O365990','O365991','O365992','O365993','O365994','O365995','O365999','O3660X0','O3660X1','O3660X2','O3660X3','O3660X4','O3660X5','O3660X9','O3661X0','O3661X1','O3661X2','O3661X3','O3661X4','O3661X5','O3661X9','O3662X0','O3662X1','O3662X2','O3662X3','O3662X4','O3662X5','O3662X9','O3663X0','O3663X1','O3663X2','O3663X3','O3663X4','O3663X5','O3663X9','O3670X0','O3670X1','O3670X2','O3670X3','O3670X4','O3670X5','O3670X9','O3671X0','O3671X1','O3671X2','O3671X3','O3671X4','O3671X5','O3671X9','O3672X0','O3672X1','O3672X2','O3672X3','O3672X4','O3672X5','O3672X9','O3673X0','O3673X1','O3673X2','O3673X3','O3673X4','O3673X5','O3673X9','O3680X0','O3680X1','O3680X2','O3680X3','O3680X4','O3680X5','O3680X9','O368120','O368121','O368122','O368123','O368124','O368125','O368129','O368130','O368131','O368132','O368133','O368134','O368135','O368139','O368190','O368191','O368192','O368193','O368194','O368195','O368199','O368210','O368211','O368212','O368213','O368214','O368215','O368219','O368220','O368221','O368222','O368223','O368224','O368225','O368229','O368230','O368231','O368232','O368233','O368234','O368235','O368239','O368290','O368291','O368292','O368293','O368294','O368295','O368299','O368310','O368311','O368312','O368313','O368314','O368315','O368319','O368320','O368321','O368322','O368323','O368324','O368325','O368329','O368330','O368331','O368332','O368333','O368334','O368335','O368339','O368390','O368391','O368392','O368393','O368394','O368395','O368399','O368910','O368911','O368912','O368913','O368914','O368915','O368919','O368920','O368921','O368922','O368923','O368924','O368925','O368929','O368930','O368931','O368932','O368933','O368934','O368935','O368939','O368990','O368991','O368992','O368993','O368994','O368995','O368999','O3690X0','O3690X1','O3690X2','O3690X3','O3690X4','O3690X5','O3690X9','O3691X0','O3691X1','O3691X2','O3691X3','O3691X4','O3691X5','O3691X9','O3692X0','O3692X1','O3692X2','O3692X3','O3692X4','O3692X5','O3692X9','O3693X0','O3693X1','O3693X2','O3693X3','O3693X4','O3693X5','O3693X9','O401XX0','O401XX1','O401XX2','O401XX3','O401XX4','O401XX5','O401XX9','O402XX0','O402XX1','O402XX2','O402XX3','O402XX4','O402XX5','O402XX9','O403XX0','O403XX1','O403XX2','O403XX3','O403XX4','O403XX5','O403XX9','O409XX0','O409XX1','O409XX2','O409XX3','O409XX4','O409XX5','O409XX9','O4100X0','O4100X1','O4100X2','O4100X3','O4100X4','O4100X5','O4100X9','O4101X0','O4101X1','O4101X2','O4101X3','O4101X4','O4101X5','O4101X9','O4102X0','O4102X1','O4102X2','O4102X3','O4102X4','O4102X5','O4102X9','O4103X0','O4103X1','O4103X2','O4103X3','O4103X4','O4103X5','O4103X9','O411010','O411011','O411012','O411013','O411014','O411015','O411019','O411020','O411021','O411022','O411023','O411024','O411025','O411029','O411030','O411031','O411032','O411033','O411034','O411035','O411039','O411090','O411091','O411092','O411093','O411094','O411095','O411099','O411210','O411211','O411212','O411213','O411214','O411215','O411219','O411220','O411221','O411222','O411223','O411224','O411225','O411229','O411230','O411231','O411232','O411233','O411234','O411235','O411239','O411290','O411291','O411292','O411293','O411294','O411295','O411299','O411410','O411411','O411412','O411413','O411414','O411415','O411419','O411420','O411421','O411422','O411423','O411424','O411425','O411429','O411430','O411431','O411432','O411433','O411434','O411435','O411439','O411490','O411491','O411492','O411493','O411494','O411495','O411499','O418X10','O418X11','O418X12','O418X13','O418X14','O418X15','O418X19','O418X20','O418X21','O418X22','O418X23','O418X24','O418X25','O418X29','O418X30','O418X31','O418X32','O418X33','O418X34','O418X35','O418X39','O418X90','O418X91','O418X92','O418X93','O418X94','O418X95','O418X99','O4190X0','O4190X1','O4190X2','O4190X3','O4190X4','O4190X5','O4190X9','O4191X0','O4191X1','O4191X2','O4191X3','O4191X4','O4191X5','O4191X9','O4192X0','O4192X1','O4192X2','O4192X3','O4192X4','O4192X5','O4192X9','O4193X0','O4193X1','O4193X2','O4193X3','O4193X4','O4193X5','O4193X9','O4200','O42011','O42012','O42013','O42019','O4202','O4210','O42111','O42112','O42113','O42119','O4212','O4290','O42911','O42912','O42913','O42919','O4292','O43011','O43012','O43013','O43019','O43021','O43022','O43023','O43029','O43101','O43102','O43103','O43109','O43111','O43112','O43113','O43119','O43121','O43122','O43123','O43129','O43191','O43192','O43193','O43199','O43211','O43212','O43213','O43219','O43221','O43222','O43223','O43229','O43231','O43232','O43233','O43239','O43811','O43812','O43813','O43819','O43891','O43892','O43893','O43899','O4390','O4391','O4392','O4393','O4400','O4401','O4402','O4403','O4410','O4411','O4412','O4413','O4420','O4421','O4422','O4423','O4430','O4431','O4432','O4433','O4440','O4441','O4442','O4443','O4450','O4451','O4452','O4453','O45001','O45002','O45003','O45009','O45011','O45012','O45013','O45019','O45021','O45022','O45023','O45029','O45091','O45092','O45093','O45099','O458X1','O458X2','O458X3','O458X9','O4590','O4591','O4592','O4593','O46001','O46002','O46003','O46009','O46011','O46012','O46013','O46019','O46021','O46022','O46023','O46029','O46091','O46092','O46093','O46099','O468X1','O468X2','O468X3','O468X9','O4690','O4691','O4692','O4693','O4700','O4702','O4703','O471','O479','O480','O481','O6000','O6002','O6003','O6010X0','O6010X1','O6010X2','O6010X3','O6010X4','O6010X5','O6010X9','O6012X0','O6012X1','O6012X2','O6012X3','O6012X4','O6012X5','O6012X9','O6013X0','O6013X1','O6013X2','O6013X3','O6013X4','O6013X5','O6013X9','O6014X0','O6014X1','O6014X2','O6014X3','O6014X4','O6014X5','O6014X9','O6020X0','O6020X1','O6020X2','O6020X3','O6020X4','O6020X5','O6020X9','O6022X0','O6022X1','O6022X2','O6022X3','O6022X4','O6022X5','O6022X9','O6023X0','O6023X1','O6023X2','O6023X3','O6023X4','O6023X5','O6023X9','O610','O611','O618','O619','O620','O621','O622','O623','O624','O628','O629','O630','O631','O632','O639','O640XX0','O640XX1','O640XX2','O640XX3','O640XX4','O640XX5','O640XX9','O641XX0','O641XX1','O641XX2','O641XX3','O641XX4','O641XX5','O641XX9','O642XX0','O642XX1','O642XX2','O642XX3','O642XX4','O642XX5','O642XX9','O643XX0','O643XX1','O643XX2','O643XX3','O643XX4','O643XX5','O643XX9','O644XX0','O644XX1','O644XX2','O644XX3','O644XX4','O644XX5','O644XX9','O645XX0','O645XX1','O645XX2','O645XX3','O645XX4','O645XX5','O645XX9','O648XX0','O648XX1','O648XX2','O648XX3','O648XX4','O648XX5','O648XX9','O649XX0','O649XX1','O649XX2','O649XX3','O649XX4','O649XX5','O649XX9','O650','O651','O652','O653','O654','O655','O658','O659','O660','O661','O662','O663','O6640','O6641','O665','O666','O668','O669','O670','O678','O679','O68','O690XX0','O690XX1','O690XX2','O690XX3','O690XX4','O690XX5','O690XX9','O691XX0','O691XX1','O691XX2','O691XX3','O691XX4','O691XX5','O691XX9','O692XX0','O692XX1','O692XX2','O692XX3','O692XX4','O692XX5','O692XX9','O693XX0','O693XX1','O693XX2','O693XX3','O693XX4','O693XX5','O693XX9','O694XX0','O694XX1','O694XX2','O694XX3','O694XX4','O694XX5','O694XX9','O695XX0','O695XX1','O695XX2','O695XX3','O695XX4','O695XX5','O695XX9','O6981X0','O6981X1','O6981X2','O6981X3','O6981X4','O6981X5','O6981X9','O6982X0','O6982X1','O6982X2','O6982X3','O6982X4','O6982X5','O6982X9','O6989X0','O6989X1','O6989X2','O6989X3','O6989X4','O6989X5','O6989X9','O699XX0','O699XX1','O699XX2','O699XX3','O699XX4','O699XX5','O699XX9','O700','O701','O702','O7020','O7021','O7022','O7023','O703','O704','O709','O7100','O7102','O7103','O711','O712','O713','O714','O715','O716','O717','O7181','O7182','O7189','O719','O720','O721','O722','O723','O730','O731','O740','O741','O742','O743','O744','O745','O746','O747','O748','O749','O750','O751','O752','O753','O754','O755','O7581','O7582','O7589','O759','O76','O770','O771','O778','O779','O80','O82','O85','O860','O8600','O8601','O8602','O8603','O8604','O8609','O8611','O8612','O8613','O8619','O8620','O8621','O8622','O8629','O864','O8681','O8689','O870','O871','O872','O873','O874','O878','O879','O88011','O88012','O88013','O88019','O8802','O8803','O88111','O88112','O88113','O88119','O8812','O8813','O88211','O88212','O88213','O88219','O8822','O8823','O88311','O88312','O88313','O88319','O8832','O8833','O88811','O88812','O88813','O88819','O8882','O8883','O8901','O8909','O891','O892','O893','O894','O895','O896','O898','O899','O900','O901','O902','O903','O904','O905','O906','O9081','O9089','O909','O91011','O91012','O91013','O91019','O9102','O9103','O91111','O91112','O91113','O91119','O9112','O9113','O91211','O91212','O91213','O91219','O9122','O9123','O92011','O92012','O92013','O92019','O9202','O9203','O92111','O92112','O92113','O92119','O9212','O9213','O9220','O9229','O923','O924','O925','O926','O9270','O9279','O94','O98011','O98012','O98013','O98019','O9802','O9803','O98111','O98112','O98113','O98119','O9812','O9813','O98211','O98212','O98213','O98219','O9822','O9823','O98311','O98312','O98313','O98319','O9832','O9833','O98411','O98412','O98413','O98419','O9842','O9843','O98511','O98512','O98513','O98519','O9852','O9853','O98611','O98612','O98613','O98619','O9862','O9863','O98711','O98712','O98713','O98719','O9872','O9873','O98811','O98812','O98813','O98819','O9882','O9883','O98911','O98912','O98913','O98919','O9892','O9893','O99011','O99012','O99013','O99019','O9902','O9903','O99111','O99112','O99113','O99119','O9912','O9913','O99210','O99211','O99212','O99213','O99214','O99215','O99280','O99281','O99282','O99283','O99284','O99285','O99310','O99311','O99312','O99313','O99314','O99315','O99320','O99321','O99322','O99323','O99324','O99325','O99330','O99331','O99332','O99333','O99334','O99335','O99340','O99341','O99342','O99343','O99344','O99345','O99350','O99351','O99352','O99353','O99354','O99355','O99411','O99412','O99413','O99419','O9942','O9943','O99511','O99512','O99513','O99519','O9952','O9953','O99611','O99612','O99613','O99619','O9962','O9963','O99711','O99712','O99713','O99719','O9972','O9973','O99810','O99814','O99815','O99820','O99824','O99825','O99830','O99834','O99835','O99840','O99841','O99842','O99843','O99844','O99845','O9989','O99891','O99892','O99893','O9A111','O9A112','O9A113','O9A119','O9A12','O9A13','O9A211','O9A212','O9A213','O9A219','O9A22','O9A23','O9A311','O9A312','O9A313','O9A319','O9A32','O9A33','O9A411','O9A412','O9A413','O9A419','O9A42','O9A43','O9A511','O9A512','O9A513','O9A519','O9A52','O9A53','Z332','Z390','Z640','P353','P354','P358','P359','P360','P3610','P3619','P362','P3630','P3639','P364','P365','P368','P369','P370','P371','P372','P373','P374','P375','P378','P379','P381','P389','P390','P391','P392','P393','P394','P398','P399','P500','P501','P502','P503','P504','P505','P508','P509','P510','P518','P519','P520','P521','P5221','P5222','P523','P524','P525','P526','P528','P529','P53','P540','P541','P542','P543','P544','P545','P546','P548','P549','P550','P551','P558','P559','P560','P5690','P5699','P570','P578','P579','P580','P581','P582','P583','P5841','P5842','P585','P588','P589','P590','P591','P5920','P5929','P593','P598','P599','P60','P610','P611','P612','P613','P614','P615','P616','P618','P619','P700','P701','P702','P703','P704','P708','P709','P710','P711','P712','P713','P714','P718','P719','P720','P721','P722','P728','P729','P740','P741','P742','P7421','P7422','P743','P7431','P7432','P744','P7441','P74421','P74422','P7449','P745','P746','P748','P749','P760','P761','P762','P768','P769','P771','P772','P773','P779','P780','P781','P782','P783','P7881','P7882','P7883','P7884','P7889','P789','P800','P808','P809','P810','P818','P819','P830','P831','P832','P8330','P8339','P834','P835','P836','P838','P8381','P8388','P839','P84','P90','P910','P911','P913','P914','P915','P9160','P9161','P9162','P9163','P918','P91811','P91819','P91821','P91822','P91823','P91829','P9188','P919','P9201','P9209','P921','P922','P923','P924','P925','P928','P929','P930','P938','P940','P941','P942','P948','P949','P95','P960','P961','P962','P963','P965','P9681','P9682','P9683','P9689','P969','Q860','Q861','Q862','Q868','Z3800','Z3801','Z381','Z382','Z3830','Z3831','Z384','Z385','Z3861','Z3862','Z3863','Z3864','Z3865','Z3866','Z3868','Z3869','Z387','Z388') then 1
-- MAGIC   else 0
-- MAGIC   end as mdc,
-- MAGIC
-- MAGIC   case
-- MAGIC    when c.icd_code_proc in ('02YA0Z0','02YA0Z2','0BYC0Z0', '0BYC0Z2', '0BYD0Z0','0BYD0Z2', '0BYF0Z0','0BYF0Z2','0BYG0Z0','0BYG0Z2','0BYH0Z0','0BYH0Z2','0BYJ0Z0','0BYJ0Z2','0BYK0Z0', '0BYK0Z2','0BYL0Z0','0BYL0Z2','0BYM0Z0','0BYM0Z2','0DY50Z0','0DY50Z2','0DY60Z0','0DY60Z2','0DY80Z0', '0DY80Z2','0DYE0Z0','0DYE0Z2','0FY00Z0','0FY00Z2','0FYG0Z0','0FYG0Z2','0TY00Z0','0TY00Z2','0TY10Z0','0TY10Z2','0WY20Z0','0XYJ0Z0','0XYK0Z0','30230AZ','30230G0', '30230G1','30230G2','30230G3','30230G4','30230U2','30230U3','30230U4','30230X0','30230X1','30230X2','30230X3','30230X4','30230Y0','30230Y1','30230Y2','30230Y3','30230Y4','30233AZ','30233G0','30233G1','30233G2','30233G3','30233G4','30233U2','30233U3','30233U4','30233X0','30233X1','30233X2','30233X3','30233X4','30233Y0','30233Y1','30233Y2','30233Y3','30233Y4','30240AZ','30240G0','30240G1','30240G2', '30240G3','30240G4','30240U2','30240U3','30240U4','30240X0','30240X1','30240X2','30240X3','30240X4','30240Y0','30240Y1','30240Y2','30240Y3','30240Y4','30243AZ','30243G0','30243G1','30243G2','30243G3','30243G4','30243U2','30243U3','30243U4','30243X0','30243X1','30243X2','30243X3','30243X4','30243Y0','30243Y1','30243Y2','30243Y3','30243Y4','3E03005','3E0300P','3E030U1','3E030WL','3E03305','3E0330P','3E033U1','3E033WL','3E04005','3E0400P','3E040WL','3E04305','3E0430P','3E043WL','3E0A305','3E0J3U1','3E0J7U1','3E0J8U1','XW01318','XW01348','XW03336','XW03351','XW03358','XW03368','XW03378','XW03387','XW03388','XW033B3','XW033C6','XW033D6','XW033H7','XW033J7','XW033K7','XW033M7','XW033N7','XW033S5','XW04336','XW04351','XW04358','XW04368','XW04378','XW04387','XW04388','XW043B3','XW043C6','XW043D6','XW043H7','XW043J7','XW043K7','XW043M7','XW043N7','XW043S5','XW133B8', 'XW143B8','XW143C8','XW23346','XW23376','XW24346','XW24376','XW133C8') then 1
-- MAGIC   else 0
-- MAGIC   end as immunip,
-- MAGIC
-- MAGIC case 
-- MAGIC when b.icd_code in ('B20','B59','C802','C888','C9440','C9441','C9442','C946','D4622','D4701','D4702','D4709','D471','D479','D47Z1','D47Z2','D47Z9','D6109','D61810','D61811','D61818','D700','D701','D702','D704','D708','D709','D71','D720','D72810','D72818','D72819','D7381','D7581','D761','D762','D763','D800','D801','D802','D803','D804','D805','D806','D807','D808','D809','D810','D811','D812','D8130','D8131','D8132','D8139','D814','D816','D817','D8182','D8189','D819','D820','D821','D822','D823','D824','D828','D829','D830','D831','D832','D838','D839','D840','D841','D848','D8481','D84821','D84822','D8489','D849','D893','D89810','D89811','D89812','D89813','D8982','D8989','D899','E40','E41','E42','E43','I120','I1311','I132','K912','N185','N186','T8600','T8601','T8602','T8603','T8609','T8610','T8611','T8612','T8613','T8619','T8620','T8621','T8622','T8623','T86290','T86298','T8630','T8631','T8632','T8633','T8639','T8640','T8641','T8642','T8643','T8649','T865','T86810','T86811','T86812','T86818','T86819','T86850','T86851','T86852','T86858','T86859','T86890','T86891','T86892','T86898','T86899','T8690','T8691','T8692','T8693','T8699','Z4821','Z4822','Z4823','Z4824','Z48280','Z48288','Z48290','Z48298','Z4901','Z4902','Z4931','Z4932','Z940','Z941','Z942','Z943','Z944','Z9481','Z9482','Z9483','Z9484','Z9489','Z992') then 1
-- MAGIC else 0
-- MAGIC end as immunid
-- MAGIC
-- MAGIC           
-- MAGIC from edav_prd_cdh.cdh_premier_v2.patdemo a
-- MAGIC left join (select pat_key,replace(icd_code,'.',"") as icd_code,icd_pri_sec from cdh_premier_V2.paticd_diag) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC left join (select pat_key, icd_code as icd_code_proc from edav_prd_cdh.cdh_premier_v2.paticd_proc) c
-- MAGIC       on a.pat_key = c.pat_key
-- MAGIC       
-- MAGIC )
-- MAGIC   
-- MAGIC where traumid = 1 OR canceid = 1 OR immunip = 1 OR immunid = 1 OR mdc = 1)
-- MAGIC
-- MAGIC
-- MAGIC   select * 
-- MAGIC   from include
-- MAGIC   where 1=1
-- MAGIC         and point_of_origin not in (2,3,4,5,6) 
-- MAGIC         and ms_drg != 999 
-- MAGIC         and pat_key not in (select pat_key from exclude)
-- MAGIC         and lowmodr = 1
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC #g.createOrReplaceTempView('ahrq_psi')	
-- MAGIC g.write.mode('overwrite').saveAsTable('cdh_premier_exploratory.xud4_ahrq_psi')

-- COMMAND ----------

select distinct pat_key, medrec_key, lowmodr
from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Matched Outcome Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CCSR Test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD (Stratified by age)

-- COMMAND ----------

--Top 5 CCSR
--Disability m_c201623
 with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623 c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr 
                  where ccsr_category != 'PRG023'
                  order by rn desc
                  limit 20
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--TOP CCSR
--Non-Disability c201623_ND
 with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr 
                  where ccsr_category in ('INF003', 'INF002', 'NVS009', 'END003','EXT014', 'INJ022', 'MBD017', 'CIR008', 'CIR019', 'RSP010','RSP002') 
                  --order by rn desc
                 -- limit 20
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD (Stratified by age)
-- MAGIC

-- COMMAND ----------

--Top 5 CCSR
--Disability m_c201623_idd
  with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p 
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr
                  where ccsr_category in ('INF003', 'INF002', 'NVS009', 'END003','EXT014', 'INJ022', 'MBD017', 'CIR008', 'CIR019', 'RSP010','RSP002') 
                  --order by rn desc
                  --limit 20
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC #### No IDD (Stratified by age)

-- COMMAND ----------

--TOP CCSR
--Non-Disability c201623_ND_idd

  with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr
                  where ccsr_category in ('INF003', 'INF002', 'NVS009', 'END003','EXT014', 'INJ022', 'MBD017', 'CIR008', 'CIR019', 'RSP010','RSP002') 
                  --order by rn desc
                  --limit 20
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD (Stratified by age)
-- MAGIC

-- COMMAND ----------

--Top 5 CCSR
--Disability m_c201623_sldd

 with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr 
                  where ccsr_category in ('INF003', 'INF002', 'NVS009', 'END003','EXT014', 'INJ022', 'MBD017', 'CIR008', 'CIR019', 'RSP010','RSP002') 
                  --order by rn desc
                  --limit 20
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD (Stratified by age)

-- COMMAND ----------

--TOP CCSR
--Non-Disability c201623_ND_sldd

 with ccsr as  (select *, count(medrec_key) over(partition by CCSR_Category) as rn
                from(select distinct c.medrec_key,c.pat_key,agem,CCSR_Category,CCSR_Category_Description
                     from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd c 
                     left join edav_prd_cdh.cdh_premier_exploratory.xud4_pdx p 
                     on p.pat_key=c.pat_key 
                    )
              
               ),

     top5ccsr as (select distinct CCSR_Category,CCSR_Category_Description,rn 
                  from ccsr
                   where ccsr_category in ('INF003', 'INF002', 'NVS009', 'END003','EXT014', 'INJ022', 'MBD017', 'CIR008', 'CIR019', 'RSP010','RSP002')
                 
                  -- order by rn desc
                  
                 ),
            
     pat as (select CCSR_Category,CCSR_Category_Description,medrec_key,
              case 
               when agem between 18 and 64 then 'a18-64'
               when agem >=65 then 'b65+'
               else 'cunknown'
              end as agegrp2
              from ccsr 
              where CCSR_Category in (select CCSR_Category from top5ccsr)
           )


       select agegrp2,CCSR_Category,CCSR_Category_Description,count(medrec_key) as report
       from pat
       group by agegrp2,CCSR_Category,CCSR_Category_Description having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Median Length of Stay Test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--los
--Inpatients with disability
    select
    agegrp2,
    median(los) as median,
    'disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

--los
--Inpatients with non-disability
    select
    agegrp2,
    median(los) as median,
    'non_disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD (Stratified by age)

-- COMMAND ----------

--los
--Inpatients with disability
    select
    agegrp2,
    median(los) as median,
    'disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

--los
--Inpatients with non-disability
    select
    agegrp2,
    median(los) as median,
    'non_disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD (Stratified by age)

-- COMMAND ----------

--los
--Inpatients with disability
    select
    agegrp2,
    median(los) as median,
    'disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

--los
--Inpatients with non-disability
    select
    agegrp2,
    median(los) as median,
    'non_disb' as t
    
    from(
      select distinct medrec_key,pat_key,los,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
      order by medrec_key
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Discharge Disposition 
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients with disability

create or replace temporary view disc_idd
as (
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    
    
    union
    
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    
    order by disc_status_desc, agegrp2
    
  );

select * from disc_idd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients with disability

create or replace temporary view disc_nd_idd
as (
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by disc_status_desc, agegrp2);

select * from disc_nd_idd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Chi-square Test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create combined Python dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC disc_idd = spark.sql("""
-- MAGIC                         select * from disc_idd
-- MAGIC                         """).toPandas()
-- MAGIC
-- MAGIC display(disc_idd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create contingency table and chi-square for loop

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC unique_combinations = disc_idd[['disc_status_desc', 'agegrp2']].drop_duplicates()
-- MAGIC print(unique_combinations)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in disc_idd['agegrp2'].unique():
-- MAGIC     df_filtered = uti[uti['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients with disability
create or replace temporary view disc_sldd 
as (
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
      order by medrec_key
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by disc_status_desc, agegrp2);

select * from disc_sldd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients without disability
create or replace temporary view disc_nd_sldd
as (
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
      order by medrec_key
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by disc_status_desc, agegrp2);

select * from disc_nd_sldd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create combined Pandas dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC disc_sldd_df = spark.sql("""
-- MAGIC                         select *, 'SLDD' as dis from disc_sldd
-- MAGIC                         """)
-- MAGIC disc_nd_sldd_df = spark.sql("""
-- MAGIC                            select *, 'No SLDD' as dis from disc_nd_sldd
-- MAGIC                            """)
-- MAGIC union_df_sldd = disc_sldd_df.union(disc_nd_sldd_df).toPandas()
-- MAGIC
-- MAGIC display(union_df_sldd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create contingency table and chi square

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for status in union_df_sldd['disc_status_desc'].unique():
-- MAGIC     df_filtered = union_df_sldd[union_df_sldd['disc_status_desc'] == status]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['dis'],
-- MAGIC         columns = ['agegrp2'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC
-- MAGIC     print(f'Contingency table for {status}:')
-- MAGIC     print(contingency_table)
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'disc_status_desc': status,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients with disability

create or replace temporary view disc 
as (
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
      order by medrec_key
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by disc_status_desc, agegrp2);

select * from disc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--Discharge status
--Inpatients with disability
create or replace temporary view disc_nd
as(
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key) as count
    
    from(select distinct medrec_key, disc_status, disc_status_desc,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
      order by medrec_key
    )
    group by agegrp2,disc_status, disc_status_desc 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by disc_status_desc, agegrp2);
select * from disc_nd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create combined Pandas dataframe 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC disc_df = spark.sql("""
-- MAGIC                         select *, 'IDD/SLDD' as dis from disc
-- MAGIC                         """)
-- MAGIC disc_nd_df = spark.sql("""
-- MAGIC                            select *, 'No IDD/SLDD' as dis from disc_nd
-- MAGIC                            """)
-- MAGIC union_df_idd_sldd = disc_df.union(disc_nd_df).toPandas()
-- MAGIC
-- MAGIC display(union_df_idd_sldd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create contigency table and chi-sqaure 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for status in union_df_idd_sldd['disc_status_desc'].unique():
-- MAGIC     df_filtered = union_df_idd_sldd[union_df_idd_sldd['disc_status_desc'] == status]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['dis'],
-- MAGIC         columns = ['agegrp2'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC
-- MAGIC     print(f'Contingency table for {status}:')
-- MAGIC     print(contingency_table)
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'disc_status_desc': status,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 30-day readmission

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD (Stratified by age)

-- COMMAND ----------

--30 day readmission
--Inpatients with disability
create or replace temporary view t30rdm_idd

as (
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
);

select * from t30rdm_idd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### To Pandas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC rdm_idd = spark.sql("""
-- MAGIC                         select * from t30rdm_idd
-- MAGIC                         """).toPandas()
-- MAGIC
-- MAGIC display(rdm_idd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Chi-square 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     rdm_idd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Disregard: No 30 day readmission

-- COMMAND ----------

--30 day readmission
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD (Stratified by age)

-- COMMAND ----------

--30 day readmission
--Inpatients with disability

create or replace temporary view t30rdm_sldd 
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
);

select * from t30rdm_sldd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### To Pandas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rdm_sldd = spark.sql("""select * from t30rdm_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Chi-Square

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     rdm_sldd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Disregard

-- COMMAND ----------

--30 day readmission
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by age)

-- COMMAND ----------


--30 day readmission
--Inpatients with disability
create or replace temporary view t30rdm
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where medrec_key in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
);

select * from t30rdm

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### To Pandas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t30rdm = spark.sql("""select * from t30rdm""").toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Chi-sqaure

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     t30rdm,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Disregard 

-- COMMAND ----------


--30 day readmission
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as Readmissionreport,
    'non_dis' as t
    
    from(
      select distinct medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where medrec_key not in  (select medrec_key from edav_prd_cdh.cdh_premier_exploratory.xud4_t30rdm)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ICU Admission

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD (Stratified by age)

-- COMMAND ----------

--ICU Admission
--Inpatients with disability
create or replace temporary view icu_idd
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
    );
    
    select * from icu_idd 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC icu_idd = spark.sql(""" select * from icu_idd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     icu_idd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

--ICU Admission
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     t30rdm,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD (Stratified by age)

-- COMMAND ----------

--ICU Admission
--Inpatients with disability
create or replace temporary view icu_sldd
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
);

select * from icu_sldd

-- COMMAND ----------

-- MAGIC %python
-- MAGIC icu_sldd = spark.sql("""select * from icu_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     icu_sldd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['agegrp2'],
-- MAGIC     columns = ['t'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Disregard

-- COMMAND ----------

--ICU Admission
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--ICU
--Inpatients with disability
create or replace temporary view icu
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where icu = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
);

select * from icu

-- COMMAND ----------

-- MAGIC %python
-- MAGIC icu =spark.sql("""select * from icu""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     icu,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

--ICU
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as ICU_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, icu,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where icu = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IMV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD (Stratified by age)

-- COMMAND ----------

--IMV
--Inpatients with disability
create or replace temporary view vent_idd
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
  );

  select * from vent_idd

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vent_idd = spark.sql("""select * from vent_idd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     vent_idd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------



-- COMMAND ----------

--IMV
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD (Stratified by age)

-- COMMAND ----------

--IMV
--Inpatients with disability
create or replace temporary view vent_sldd
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
    );
    
    select * from vent_sldd

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vent_sldd = spark.sql("""select * from vent_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     vent_sldd,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

--IMV
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--IMV
--Inpatients with disability
create or replace temporary view vent
as(
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as count,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where vent = 1
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
    );
    
    select * from vent

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vent = spark.sql("""select * from vent""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC contingency_table = pd.pivot_table(
-- MAGIC     vent,
-- MAGIC     values = 'count',
-- MAGIC     index = ['t'],
-- MAGIC     columns = ['agegrp2'],
-- MAGIC     aggfunc = sum,
-- MAGIC     fill_value = 0)
-- MAGIC
-- MAGIC print(contingency_table)
-- MAGIC
-- MAGIC chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC results.append({'p_value': p,
-- MAGIC                 'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

--IMV
--Inpatients with disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct medrec_key) as vent_report,
    'non_dis' as t
    
    from(
      select distinct medrec_key, vent,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where vent = 0
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AHRQ Hospitalization Level

-- COMMAND ----------

--Matched and Unmatched Dataset Totals
/*
select count (distinct pat_key) as hosp_count, 'IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623

union

select count (distinct pat_key) as hosp_count, 'No IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND

union

select count (distinct pat_key) as hosp_count, 'IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd

union

select count (distinct pat_key) as hosp_count, 'SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd

union 
*/
select count (distinct pat_key) as hosp_count, 'm_IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623

union

select count (distinct pat_key) as hosp_count, 'm_No IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND

union

select count (distinct pat_key) as hosp_count, 'm_IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd

union

select count (distinct pat_key) as hosp_count, 'm_No IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd

union

select count (distinct pat_key) as hosp_count, 'm_SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd

union


select count (distinct pat_key) as hosp_count, 'm_No SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UTI
-- MAGIC

-- COMMAND ----------

select distinct pat_key, medrec_key, uti
from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi
limit 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by Age)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### UTI

-- COMMAND ----------

/*
select
    agegrp2,
    count(distinct pat_key) as uti_report,
    'dis' as t,
    'uti' as a
    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623 a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi b
      on a.pat_key = b.pat_key
      where uti = 1 )  
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
  */

-- COMMAND ----------

--UTI

--Inpatients with disability - UTI

create or replace temporary view uti 
as(
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union
--No UTI

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
   

);

select * from uti 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC uti = spark.sql("""select * from uti""").toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Chi-square

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in uti['agegrp2'].unique():
-- MAGIC     df_filtered = uti[uti['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### No UTI 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### IDD & No IDD (Stratified by Age)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### UTI

-- COMMAND ----------

--UTI

--Inpatients with disability
create or replace temporary view uti_idd
as(
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union
--No UTI

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
   
);
select * from uti_idd
   


-- COMMAND ----------

-- MAGIC %python
-- MAGIC uti_idd = spark.sql("""select * from uti_idd""").toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Chi-square

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in uti_idd['agegrp2'].unique():
-- MAGIC     df_filtered = uti_idd[uti_idd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### No UTI
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SLDD & No SLDD (Stratified by Age)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### UTI

-- COMMAND ----------

--UTI

create or replace temporary view uti_sldd
as(
--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union
    --No UTI

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where uti=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
    

);

select * from uti_sldd


-- COMMAND ----------

-- MAGIC %python
-- MAGIC uti_sldd = spark.sql("""select * from uti_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in uti_sldd['agegrp2'].unique():
-- MAGIC     df_filtered = uti_sldd[uti_sldd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### No UTI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Uncontrolled Diabetes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### IDD/SLDD & No IDD/SLDD (Stratified by age)

-- COMMAND ----------

--Uncontrolled Diabetes
create or replace temporary view diabetes
as (
--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

union

--Uncontrolled Diabetes

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as diabetes_report,
    'dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    'non_dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes= 0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
   

);
select * from diabetes


-- COMMAND ----------

-- MAGIC %python
-- MAGIC diabetes = spark.sql("""select * from diabetes""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in diabetes['agegrp2'].unique():
-- MAGIC     df_filtered = diabetes[diabetes['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### IDD & No IDD (Stratified by age)

-- COMMAND ----------

--Uncontrolled Diabetes

create or replace temporary view diabetes_idd
as (
--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union 

    --Uncontrolled Diabetes

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as diabetes_report,
    'dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    'non_dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
   

); 
select * from diabetes_idd 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC diabetes_idd = spark.sql("""select * from diabetes_idd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in diabetes_idd['agegrp2'].unique():
-- MAGIC     df_filtered = diabetes_idd[diabetes_idd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SLDD & No SLDD (Stratified by age)

-- COMMAND ----------


--Uncontrolled Diabetes
create or replace temporary view diabetes_sldd
as(
--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union
    
--Uncontrolled Diabetes

--Inpatients with disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no diabetes' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where diabetes=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')
   
);

select * from diabetes_sldd


-- COMMAND ----------

-- MAGIC %python
-- MAGIC diabetes_sldd = spark.sql("""select * from diabetes_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in diabetes_sldd['agegrp2'].unique():
-- MAGIC     df_filtered = diabetes_sldd[diabetes_sldd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Community-Acquired Pneumonia
-- MAGIC

-- COMMAND ----------

--Community-Acquired Pneumonia
create or replace temporary view pneumonia
as(
--Inpatients with disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with disability - no pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

);
select * from pneumonia


-- COMMAND ----------

-- MAGIC %python
-- MAGIC pneumonia = spark.sql("""select * from pneumonia""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in pneumonia['agegrp2'].unique():
-- MAGIC     df_filtered = pneumonia[pneumonia['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

--Community-Acquired Pneumonia
create or replace temporary view pneumonia_idd
as(
--Inpatients with disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with disability - no pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

);
select * from pneumonia_idd
   


-- COMMAND ----------

-- MAGIC %python
-- MAGIC pneumonia_idd = spark.sql("""select * from pneumonia_idd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in pneumonia_idd['agegrp2'].unique():
-- MAGIC     df_filtered = pneumonia_idd[pneumonia_idd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 
-- MAGIC                 

-- COMMAND ----------

--Community-Acquired Pneumonia
create or replace temporary view pneumonia_sldd
as(
--Inpatients with disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=1)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')


    union

    --Inpatients with disability - no pneumonia
    select
    agegrp2,
    count(distinct pat_key) as count,
    'dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability
    select
    agegrp2,
    count(distinct pat_key) as count,
    'non_dis' as t,
    'no pneumonia' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi where pneu=0)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

);

select * from pneumonia_sldd
 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC pneumonia_sldd = spark.sql("""select * from pneumonia_sldd""").toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC results = []
-- MAGIC
-- MAGIC for agegrp in pneumonia_sldd['agegrp2'].unique():
-- MAGIC     df_filtered = pneumonia_sldd[pneumonia_sldd['agegrp2'] == agegrp]
-- MAGIC
-- MAGIC     contingency_table = pd.pivot_table(
-- MAGIC         df_filtered,
-- MAGIC         values = 'count',
-- MAGIC         index = ['a'],
-- MAGIC         columns = ['t'],
-- MAGIC         aggfunc = sum,
-- MAGIC         fill_value = 0)
-- MAGIC     
-- MAGIC     print(f'Contingency Table for {agegrp}:')
-- MAGIC     
-- MAGIC     print(contingency_table)
-- MAGIC     
-- MAGIC
-- MAGIC     chi2, p, dof, expected = chi2_contingency(contingency_table)
-- MAGIC
-- MAGIC     results.append({'agegrp': agegrp,
-- MAGIC                     'p_value': p,
-- MAGIC                     'chi2_statistic': chi2})
-- MAGIC
-- MAGIC results_df = pd.DataFrame(results)
-- MAGIC
-- MAGIC display(results_df)
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Death Rate in Low-Mortality Diagnosis Related Groups

-- COMMAND ----------

select
    count(distinct pat_key) as lowmodr_report,
    'IDD/SLDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623 a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key)  
  
  union

  select
    count(distinct pat_key) as lowmodr_report,
    'no IDD/SLDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key) 

union

select
    count(distinct pat_key) as lowmodr_report,
    'IDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key) 
  
union

select
    count(distinct pat_key) as lowmodr_report,
    'No IDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key) 

union

select
    count(distinct pat_key) as lowmodr_report,
    'SLDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key) 

union

select
    count(distinct pat_key) as lowmodr_report,
    'No SLDD' as t

    
    from(
      select distinct a.pat_key, a.medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd a
      inner join edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi b
      on a.pat_key = b.pat_key) 
    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD

-- COMMAND ----------

-- Death Rate in Low-Mortality Diagnosis Related Groups

--Inpatients with disability - low modr
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20) 
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20) 
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')


   


-- COMMAND ----------

-- Death Rate in Low-Mortality Diagnosis Related Groups 

--Inpatients with disability - low modr
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')


   


-- COMMAND ----------

-- Death Rate in Low-Mortality Diagnosis Related Groups

--Inpatients with disability - death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with disability - no death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'dis' as t,
    'no death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status != 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - death
    select
    agegrp2,
    count(distinct pat_key) as lowmodr_report,
    'non_dis' as t,
    'death' as a
    
    from(
      select distinct pat_key, medrec_key,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
     where pat_key in  (select pat_key from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_psi where disc_status = 20)
    )
    group by agegrp2 having agegrp2 in ('a18-64','b65+')


   


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DHDD - Inpatient Demographics (Matched and Unmatched)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Denominators

-- COMMAND ----------

--Matched and Unmatched Dataset Totals

select count (distinct medrec_key) as pat_count, 'IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623

union

select count (distinct medrec_key) as pat_count, 'No IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND

union

select count (distinct medrec_key) as pat_count, 'IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd

union

select count (distinct medrec_key) as pat_count, 'SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd

union 

select count (distinct medrec_key) as pat_count, 'm_IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623

union

select count (distinct medrec_key) as pat_count, 'm_No IDD/SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND

union

select count (distinct medrec_key) as pat_count, 'm_IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd

union

select count (distinct medrec_key) as pat_count, 'm_No IDD' as t 
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd

union

select count (distinct medrec_key) as pat_count, 'm_SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd

union


select count (distinct medrec_key) as pat_count, 'm_No SLDD' as t
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd





-- COMMAND ----------

--Matched and Unmatched Dataset Totals

select count (distinct medrec_key) as pat_count, 'IDD/SLDD' as t, agegrp2
from (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623)
group by agegrp2
having agegrp2 in ('a18-64', 'b65+')

union

select count (distinct medrec_key) as pat_count, 'No IDD/SLDD' as t, agegrp2
from (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'IDD' as t, agegrp2 
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'SLDD' as t, agegrp2 
from (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union 

select count (distinct medrec_key) as pat_count, 'm_IDD/SLDD' as t, agegrp2 
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'm_No IDD/SLDD' as t, agegrp2 
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'm_IDD' as t, agegrp2  
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'm_No IDD' as t, agegrp2  
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct medrec_key) as pat_count, 'm_SLDD' as t, agegrp2 
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union


select count (distinct medrec_key) as pat_count, 'm_No SLDD' as t, agegrp2 
from  (select medrec_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')



-- COMMAND ----------

--Matched and Unmatched Dataset Totals


select count (distinct pat_key) as pat_count, 'm_IDD/SLDD' as t, agegrp2 
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct pat_key) as pat_count, 'm_No IDD/SLDD' as t, agegrp2 
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct pat_key) as pat_count, 'm_IDD' as t, agegrp2  
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct pat_key) as pat_count, 'm_No IDD' as t, agegrp2  
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union

select count (distinct pat_key) as pat_count, 'm_SLDD' as t, agegrp2 
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')

union


select count (distinct pat_key) as pat_count, 'm_No SLDD' as t, agegrp2 
from  (select pat_key, 
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd)
group by agegrp2
having agegrp2 in ('a18-64','b65+')



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Cleaning

-- COMMAND ----------

--Number of records with missing values
SELECT 
  SUM(CASE WHEN medrec_key IS NULL THEN 1 ELSE 0 END) AS null_medrec_key,
  SUM(CASE WHEN pat_key IS NULL THEN 1 ELSE 0 END) AS null_pat_key,
  SUM(CASE WHEN disc_status IS NULL THEN 1 ELSE 0 END) AS null_disc_status,
  SUM(CASE WHEN disc_status_desc IS NULL THEN 1 ELSE 0 END) AS null_disc_status_desc,
  SUM(CASE WHEN los IS NULL THEN 1 ELSE 0 END) AS null_los,
  SUM(CASE WHEN agem IS NULL THEN 1 ELSE 0 END) AS null_agem,
  SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END) AS null_race,
  SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) AS null_gender,
  SUM(CASE WHEN ins IS NULL THEN 1 ELSE 0 END) AS null_ins,
  SUM(CASE WHEN hispanic_ind IS NULL THEN 1 ELSE 0 END) AS null_hispanic_ind,
  SUM(CASE WHEN admit_date IS NULL THEN 1 ELSE 0 END) AS null_admit_date,
  SUM(CASE WHEN icu IS NULL THEN 1 ELSE 0 END) AS null_icu,
  SUM(CASE WHEN vent IS NULL THEN 1 ELSE 0 END) AS null_vent
FROM edav_prd_cdh.cdh_premier_exploratory.xud4_c201623;

-- COMMAND ----------

-- Data Cleaning: Identify people with multiple genders

select medrec_key, count(distinct gender) as num_gender
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
group by medrec_key
having count (distinct gender) > 1

--25 people have multiple genders listed from 2016 - 2023 among those with idd/sldd

-- COMMAND ----------

-- Identify people with multiple ethnicities

select medrec_key, count(distinct hispanic_ind) as num_ins
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
group by medrec_key
having count (distinct hispanic_ind) > 1



-- COMMAND ----------

-- Identify people with multiple insurance

select medrec_key, count(distinct ins) as num_ins
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
group by medrec_key
having count (distinct ins) > 1

--People could either be dually eligible or they could have had different insurance types over time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unmatched Demographics Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Age

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD & No IDD/SLDD 

-- COMMAND ----------

--Median age among those with IDD/SLDD
select concat("1 Median-IDD-SLDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623)
 union
--Median age among those without IDD/SLDD
 select concat("2 Median-IDD-SLDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_nd)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####IDD & No IDD

-- COMMAND ----------

--Median age among those with IDD
select concat("1 Median-IDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd)
 union
--Median age among those without IDD
 select concat("2 Median-IDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_idd)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

--Median age among those with SLDD
select concat("1 Median-SLDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd)
 union
 --Median age among those without SLDD
 select concat("2 Median-SLDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_sldd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gender

-- COMMAND ----------

--Gender at their most recent visit

--IDD/SLDD
select gender, concat("Gender-IDD-SLDD-Disability:  ",count (distinct medrec_key)) as Report 
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623)
where visnum = 1)

group by gender

union
--No IDD/SLDD
select gender, concat("Gender-IDD-SLDD-ND-Disability:  ",count (distinct medrec_key)) as Report 
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND)
where visnum = 1)

group by gender

union

--IDD
select gender, concat("Gender-IDD-Disability:  ",count (distinct medrec_key)) as Report 
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd)
where visnum = 1)

group by gender

union

--SLDD
select gender, concat("Gender-SLDD-Disability:  ",count (distinct medrec_key)) as Report 
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum -- order by most recent visit for each patient
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd)
where visnum = 1) -- limit to most recent visit

group by gender


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Race

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD 

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD_SLDD-Disability" as t 
from recent_visits 
group by rollup(race)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND
    )
where admit_date = max_admit_date)

--Race among those without IDD/SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD_SLDD-NON-Disability" as t 
from recent_visits 
group by rollup(race)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd
    )
where admit_date = max_admit_date)


--Race among those with IDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD-Disability" as t
from recent_visits 
group by rollup(race)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_idd
    )
where admit_date = max_admit_date)


--Race among those without IDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD-NON-Disability" as t
from recent_visits 
group by rollup(race)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd
    )
where admit_date = max_admit_date)

--Race among those with SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-SLDD-Disability" as t
from recent_visits 
group by rollup(race)

/*
medrec_key	num_race
39598530	2
184650457	2

39598530	W
39598530	U
184650457	W
184650457	O
*/


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_sldd
    )
where admit_date = max_admit_date)


--Race among those with SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-SLDD-NON-Disability" as t
from recent_visits 
group by rollup(race)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ethnicity 

-- COMMAND ----------

--Ethnicity at their most recent visit

--IDD/SLDD
select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-SLDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623)
where visnum = 1)

group by hispanic_ind

union

--No IDD/SLDD
select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-SLDD-ND-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND)
where visnum = 1)

group by hispanic_ind

union

--IDD

select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd)
where visnum = 1)

group by hispanic_ind

union

--SLDD
select hispanic_ind, count (distinct medrec_key) as Report, "Ethnicity-SLDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd)
where visnum = 1)

group by hispanic_ind



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insurance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623
    )
where admit_date = max_admit_date)
/*
dups as (select *
from
(select medrec_key, count(ins) as num_ins
from recent_visits
group by medrec_key
)
where num_ins > 1)
*/
select medrec_key, ins, admit_date
from recent_visits
where medrec_key in (50728182,89544921,110356417,
117966568,
187050215,
273293109,
273572360,
455024410,
80710288,
498652863,
42133205,
272625117,
402346930)



/*
--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD_SLDD-Disability" as t 
from recent_visits 
group by rollup (ins)
*/

/*
medrec_key	num_ins
50728182	2
89544921	2
110356417	2
117966568	2
187050215	2
273293109	2
273572360	2
455024410	2
80710288	2
498652863	2
42133205	2
272625117	2
402346930	2
*/



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND
    )
where admit_date = max_admit_date)

--Race among those without IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD_SLDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_idd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_idd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_sldd
    )
where admit_date = max_admit_date)

--Race among those with SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-SLDD-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_c201623_ND_sldd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-SLDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Matched Demographics Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Age

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD vs. No IDD/SLDD

-- COMMAND ----------

--Median age among those with IDD/SLDD
select concat("1 Median-IDD-SLDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623)
 union
--Median age among those without IDD/SLDD
 select concat("2 Median-IDD-SLDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD 

-- COMMAND ----------

--Median age among those with IDD
select concat("1 Median-IDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd)
 union
--Median age among those without IDD
 select concat("2 Median-IDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD 

-- COMMAND ----------

--Median age among those with SLDD
select concat("1 Median-SLDD-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd)
 union
 --Median age among those without SLDD
 select concat("2 Median-SLDD-NON-Disability: ",median(agem)) as Report from (select distinct medrec_key,agem from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gender

-- COMMAND ----------

--Gender at their most recent visit

--IDD/SLDD
select gender, count (distinct medrec_key) as Report, "Gender-IDD-SLDD-Disability" as t
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623)
where visnum = 1)

group by gender

union
--No IDD/SLDD
select gender, count (distinct medrec_key) as Report, "Gender-IDD-SLDD-ND-Disability" as t 
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND)
where visnum = 1)

group by gender

union

--IDD
select gender, count (distinct medrec_key) as Report, "Gender-IDD-Disability" as t
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd)
where visnum = 1)

group by gender

union

--No IDD
select gender, count (distinct medrec_key) as Report, "Gender-IDD-ND-Disability" as t
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd)
where visnum = 1)

group by gender

union

--SLDD
select gender, count (distinct medrec_key) as Report, "Gender-SLDD-Disability" as t
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd)
where visnum = 1)

group by gender

union

--No SLDD
select gender, count (distinct medrec_key) as Report, "Gender-SLDD-ND-Disability" as t
from
(select pat_key, medrec_key, gender, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd)
where visnum = 1)

group by gender


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Race

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD_SLDD-Disability" as t 
from recent_visits 
group by rollup(race)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND
    )
where admit_date = max_admit_date)

--Race among those without IDD/SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD_SLDD-NON-Disability" as t 
from recent_visits 
group by rollup(race)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
    )
where admit_date = max_admit_date)


--Race among those with IDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD-Disability" as t
from recent_visits 
group by rollup(race)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd
    )
where admit_date = max_admit_date)


--Race among those without IDD
select race, count(distinct medrec_key) as pat_count, "1 Race-IDD-NON-Disability" as t
from recent_visits 
group by rollup(race)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
    )
where admit_date = max_admit_date)


--Race among those with SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-SLDD-Disability" as t
from recent_visits 
group by rollup(race)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, race, admit_date
from (
  select medrec_key, race, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd
    )
where admit_date = max_admit_date)


--Race among those with SLDD
select race, count(distinct medrec_key) as pat_count, "1 Race-SLDD-NON-Disability" as t
from recent_visits 
group by rollup(race)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ethnicity

-- COMMAND ----------

--Ethnicity at their most recent visit

--IDD/SLDD
select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-SLDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623)
where visnum = 1)

group by hispanic_ind

union

--No IDD/SLDD
select hispanic_ind, count(distinct medrec_key) as Report,  "Ethnicity-IDD-SLDD-NON-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND)
where visnum = 1)

group by hispanic_ind

union

--IDD

select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd)
where visnum = 1)

group by hispanic_ind

union

--No IDD

select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-IDD-NON-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd)
where visnum = 1)

group by hispanic_ind

union

--SLDD
select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-SLDD-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd)
where visnum = 1)

group by hispanic_ind

union

--NO SLDD
select hispanic_ind, count(distinct medrec_key) as Report, "Ethnicity-SLDD-NON-Disability" as t
from
(select pat_key, medrec_key, hispanic_ind, visnum
from 
(select *,
row_number() over (partition by medrec_key order by admit_date desc) as visnum
from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd)
where visnum = 1)

group by hispanic_ind



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insurance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD 

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD_SLDD-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD
-- MAGIC
-- MAGIC

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND
    )
where admit_date = max_admit_date)

--Race among those without IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD_SLDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD  

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_idd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-IDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
    )
where admit_date = max_admit_date)

--Race among those with SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-SLDD-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD

-- COMMAND ----------

with recent_visits as 
(select distinct medrec_key, ins, admit_date
from (
  select medrec_key, ins, admit_date, max(admit_date) over (partition by medrec_key) as max_admit_date
  from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_ND_sldd
    )
where admit_date = max_admit_date)

--Race among those with IDD/SLDD
select ins, count(distinct medrec_key) as pat_count, "1 Ins-SLDD-NON-Disability" as t 
from recent_visits 
group by rollup (ins)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Trend Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Discharge Disposition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD 

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD/SLDD

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623_ND
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623_idd
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No IDD

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623_ND_idd
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623_sldd
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No SLDD

-- COMMAND ----------

--Discharge status
--Inpatients with disability
    select
    disc_status,
    disc_status_desc,
    agegrp2,
    count(distinct medrec_key),
    year
    
    from(select distinct medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
         case 
          when agem between 18 and 64 then 'a18-64'
          when agem >=65 then 'b65+'
          else 'cunknown'
        end as agegrp2

      from m_c201623_ND_sldd
    )
    group by agegrp2,disc_status, disc_status_desc,year 
    having agegrp2 in ('a18-64','b65+') and disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
    order by year, disc_status_desc, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UTI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD/SLDD & No IDD/SLDD

-- COMMAND ----------

--UTI

--Inpatients with disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623
     where pat_key in  (select pat_key from ahrq where uti=1)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND
     where pat_key in  (select pat_key from ahrq where uti=1)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

--No UTI

--Inpatients with disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'no uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IDD & No IDD

-- COMMAND ----------

--UTI

--Inpatients with disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_idd
     where medrec_key in  (select medrec_key from ahrq where uti=1)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND_idd
     where medrec_key in  (select medrec_key from ahrq where uti=1)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

--No UTI

--Inpatients with disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'no uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_idd
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND_idd
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SLDD & No SLDD

-- COMMAND ----------

--UTI

--Inpatients with disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_sldd
     where medrec_key in  (select medrec_key from ahrq where uti=1)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND_sldd
     where medrec_key in  (select medrec_key from ahrq where uti=1)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

--No UTI

--Inpatients with disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'dis' as t,
    'no uti' as a
    
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_sldd
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2, year 
    having agegrp2 in ('a18-64','b65+')

    union

    --Inpatients with non-disability - No UTI
    select
    agegrp2,
    count(distinct pat_key) as uti_report,
    year,
    'non_dis' as t,
    'no uti' as a
    
    from(
      select distinct pat_key, medrec_key, year(admit_date) as year,
      case 
       when agem between 18 and 64 then 'a18-64'
       when agem >=65 then 'b65+'
      else 'cunknown'
      end as agegrp2
      from m_c201623_ND_sldd
     where medrec_key in  (select medrec_key from ahrq where uti=0)
    )
    group by agegrp2,year 
    having agegrp2 in ('a18-64','b65+')
   


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #Data Visualization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Import Packages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # importing packages 
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC #Seaborn is a "Python data visualization library based on matplotlib"
-- MAGIC import seaborn as sns
-- MAGIC
-- MAGIC #Matplotlib is a "comprehensive library for creating static, animated, and interactive visualizations in Python"
-- MAGIC import matplotlib.pyplot as plt 
-- MAGIC
-- MAGIC #Format percent on y axis
-- MAGIC import matplotlib.ticker as mtick

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##UTI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Disability and No Disability PySpark Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test = spark.sql("""
-- MAGIC    
-- MAGIC --Inpatients with disability - UTI
-- MAGIC     
-- MAGIC
-- MAGIC       select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'IDD/SDBLD' as dis
-- MAGIC       --'IDD/SDBLD vs. No IDD/SDBLD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623 a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC      
-- MAGIC      
-- MAGIC       union
-- MAGIC
-- MAGIC       select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'No IDD/SDBLD' as dis
-- MAGIC       --'IDD/SLDD vs. No IDD/SLDD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC      
-- MAGIC
-- MAGIC      union
-- MAGIC
-- MAGIC      select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'IDD' as dis
-- MAGIC       --'IDD vs. No IDD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC      
-- MAGIC
-- MAGIC      union
-- MAGIC
-- MAGIC      select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'No IDD' as dis
-- MAGIC       --'IDD vs. No IDD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC      
-- MAGIC
-- MAGIC   union
-- MAGIC
-- MAGIC      select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'SDBLD' as dis
-- MAGIC       --'SLDD vs. No SLDD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC      
-- MAGIC      union
-- MAGIC
-- MAGIC      select distinct a.pat_key, a.medrec_key, year(admit_date) as year, coalesce(uti, 0) as uti,
-- MAGIC       case 
-- MAGIC        when agem between 18 and 64 then '18-64'
-- MAGIC        when agem >=65 then '65+'
-- MAGIC       else 'unknown'     
-- MAGIC       end as agegrp2,
-- MAGIC       'No SDBLD' as dis
-- MAGIC       --'SLDD vs. No SLDD' as group
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd a
-- MAGIC      left join (select pat_key, coalesce(uti, 0) as uti from edav_prd_cdh.cdh_premier_exploratory.xud4_ahrq_pqi) b
-- MAGIC       on a.pat_key = b.pat_key
-- MAGIC       
-- MAGIC     
-- MAGIC       """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test.createOrReplaceTempView("test")

-- COMMAND ----------

select dis, agegrp2, count(distinct pat_key)
from test
group by dis, agegrp2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Convert Spark Dataframe to Pandas Dataframe
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Convert Spark Dataframe to Pandas dataframe
-- MAGIC test_pd = test.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(test_pd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create Disability and Age Variable and Group Variable
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test_pd = test_pd.groupby(['year','uti','agegrp2','dis'])['pat_key'].nunique().reset_index(name='counts')
-- MAGIC print(test_pd)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #calculate the total number of patients for each year by dis and age category
-- MAGIC test_pd['total'] = test_pd.groupby(['year','agegrp2','dis'])['counts'].transform('sum')
-- MAGIC
-- MAGIC test_pd.head()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test_pd['proportion'] = test_pd['counts']/test_pd['total'] 
-- MAGIC display(test_pd)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test_pd2 = test_pd[test_pd['uti'] == 1]
-- MAGIC print(test_pd2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC test_pd['dis_age'] = test_pd['dis'] + '-' + test_pd['agegrp2'] 
-- MAGIC
-- MAGIC def group_dis_age(x):
-- MAGIC     if "IDD/SDBLD" in x:
-- MAGIC         return 'IDD/SDBLD vs No IDD/SDBLD'
-- MAGIC     elif 'IDD' in x and 'SDBLD' not in x:
-- MAGIC         return 'IDD vs No IDD'
-- MAGIC     elif 'SDBLD' in x and 'IDD' not in x:
-- MAGIC         return 'SDBLD vs. No SDBLD'
-- MAGIC     else:
-- MAGIC         return None
-- MAGIC     
-- MAGIC test_pd['ngroup'] = test_pd['dis_age'].apply(group_dis_age)
-- MAGIC
-- MAGIC display(test_pd)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test_pd2['dis_age'] = test_pd2['dis'] + '-' + test_pd2['agegrp2']
-- MAGIC
-- MAGIC test_pd2.loc[test_pd2['dis_age'].str.contains("IDD/SDBLD"), 'ngroup'] = 'IDD/SDBLD vs No IDD/SDBLD'
-- MAGIC test_pd2.loc[(test_pd2['dis_age'].str.contains("IDD")) & (~test_pd2['dis_age'].str.contains("SDBLD")), 'ngroup'] = 'IDD vs No IDD'
-- MAGIC test_pd2.loc[(test_pd2['dis_age'].str.contains("SDBLD")) & (~test_pd2['dis_age'].str.contains("IDD")), 'ngroup'] = 'SDBLD vs. No SDBLD'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Line Graph Facet Grid

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Define facet grid legend order
-- MAGIC desired_order = ['IDD/SDBLD-65+', 'No IDD/SDBLD-65+', 'IDD/SDBLD-18-64', 'No IDD/SDBLD-18-64', 'IDD-65+', 'IDD-18-64','No IDD-65+','No IDD-18-64','SDBLD-65+','No SDBLD-65+','SDBLD-18-64','No SDBLD-18-64']
-- MAGIC
-- MAGIC #Facet grid order
-- MAGIC col_order = ['IDD/SDBLD vs No IDD/SDBLD', 'IDD vs No IDD', 'SDBLD vs. No SDBLD']
-- MAGIC
-- MAGIC #Define palette
-- MAGIC palette = {'IDD/SDBLD-18-64': 'red',
-- MAGIC            'IDD/SDBLD-65+': 'red',
-- MAGIC            'No IDD/SDBLD-18-64': 'blue',
-- MAGIC            'No IDD/SDBLD-65+':'blue',
-- MAGIC            'IDD-65+': 'red',
-- MAGIC             'IDD-18-64': 'red',
-- MAGIC             'No IDD-65+': 'blue',
-- MAGIC             'No IDD-18-64': 'blue',
-- MAGIC             'SDBLD-65+': 'red',
-- MAGIC             'No SDBLD-65+': 'blue',
-- MAGIC             'SDBLD-18-64': 'red',
-- MAGIC             'No SDBLD-18-64': 'blue' }
-- MAGIC
-- MAGIC #Define dashes
-- MAGIC dashes = {'IDD/SDBLD-18-64': 'dashed',
-- MAGIC            'IDD/SDBLD-65+': 'solid',
-- MAGIC            'No IDD/SDBLD-18-64': 'dashed',
-- MAGIC            'No IDD/SDBLD-65+': 'solid',
-- MAGIC             'IDD-65+': 'solid',
-- MAGIC             'IDD-18-64': 'dashed',
-- MAGIC             'No IDD-65+': 'solid',
-- MAGIC             'No IDD-18-64': 'dashed',
-- MAGIC             'SDBLD-65+': 'solid',
-- MAGIC             'No SDBLD-65+': 'solid',
-- MAGIC             'SDBLD-18-64': 'dashed',
-- MAGIC             'No SDBLD-18-64': 'dashed' }
-- MAGIC
-- MAGIC #Define facet grid
-- MAGIC g = sns.FacetGrid(test_pd2, col = 'ngroup', hue = 'dis_age', hue_order = desired_order, palette = palette, col_order = col_order)
-- MAGIC
-- MAGIC #Define lineplots
-- MAGIC g.map(sns.lineplot,'year', 'proportion', errorbar=None)
-- MAGIC
-- MAGIC #Add dashed lines
-- MAGIC for ax in g.axes.flat:
-- MAGIC   for line in ax.get_lines(): 
-- MAGIC     label = line.get_label()
-- MAGIC     if label in dashes:
-- MAGIC         line.set_linestyle(dashes[label])
-- MAGIC
-- MAGIC #Legend: Add dashed lines to legend and update legend title
-- MAGIC for ax in g.axes.flat:
-- MAGIC   for line in ax.legend(title= 'Disability Status & Age',
-- MAGIC   bbox_to_anchor=(0.5,-0.3), loc='upper center').get_lines():
-- MAGIC     label = line.get_label()
-- MAGIC     if label in dashes:
-- MAGIC       line.set_linestyle(dashes[label])
-- MAGIC
-- MAGIC #Update x axis labels
-- MAGIC for ax in g.axes.flat:
-- MAGIC   years = list(range(2016, 2024, 1))
-- MAGIC   ax.set_xticks(years)
-- MAGIC   ax.set_xticklabels (years, rotation = 45)
-- MAGIC
-- MAGIC #Update table labels and titles
-- MAGIC g.set_axis_labels('Year', 'Percent of UTIs')
-- MAGIC g.set_titles('{col_name}')
-- MAGIC ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
-- MAGIC #g.fig.suptitle('Percent of UTI Hospitalizations by Year, Disability, and Age', y =1.05)
-- MAGIC
-- MAGIC plt.show()

-- COMMAND ----------


#Add dashed lines
for ax in g.axes.flat:
  for line in ax.get_lines(): 
    label = line.get_label()
    if label in dashes:
        line.set_linestyle(dashes[label])

#Legend: Add dashed lines to legend and update legend title
for ax in g.axes.flat:
  for line in ax.legend(title= 'Disability Status & Age',
  bbox_to_anchor=(0.5,-0.3), loc='upper center').get_lines():
    label = line.get_label()
    if label in dashes:
      line.set_linestyle(dashes[label])

#Update x axis labels
for ax in g.axes.flat:
  years = list(range(2016, 2024, 1))
  ax.set_xticks(years)
  ax.set_xticklabels (years, rotation = 45)

#Update table labels and titles
g.set_axis_labels('Year', 'Percent of UTIs')
g.set_titles('{col_name}')
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
#g.fig.suptitle('Percent of UTI Hospitalizations by Year, Disability, and Age', y =1.05)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print (test_pd['dis_age'].unique())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Discharge Disposition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IDD/SLDD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Disability and No Disability PySpark Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dis = spark.sql("""
-- MAGIC select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'IDD/SDBLD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC
-- MAGIC union
-- MAGIC
-- MAGIC select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'No IDD/SDBLD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert Spark Dataframe to Pandas Dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Convert Spark Dataframe to Pandas dataframe
-- MAGIC dis_pd = dis.toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Disability and Age Variable
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis_pd['dis_age'] = dis_pd['dis'] + '-' + dis_pd['agegrp2'] 
-- MAGIC dis_pd.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Numerators 
-- MAGIC
-- MAGIC Calculate numerators for Each Discharge Status grouped by year, age group, and disability status 
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis_grouped = dis_pd.groupby(['year','disc_status','agegrp2','dis'])['medrec_key'].nunique().reset_index(name='counts')
-- MAGIC print(dis_grouped)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Denominators
-- MAGIC Calculate denominators for grouped by year, age group, and disability status
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #calculate the total number of patients for each year by dis and age category
-- MAGIC dis_grouped['total'] = dis_grouped.groupby(['year','agegrp2','dis'])['counts'].transform('sum')
-- MAGIC
-- MAGIC dis_grouped.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Proportions 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis_grouped['proportion'] = dis_grouped['counts']/dis_grouped['total'] 
-- MAGIC print(dis_grouped)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Line Graph Facet Grid

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Define facet legend
-- MAGIC dis_grouped['dis_age'] = dis_grouped['dis'] + '-' + dis_grouped['agegrp2'] 
-- MAGIC
-- MAGIC #Create facet table labels
-- MAGIC dis_grouped['disc_name'] = dis_grouped['disc_status'].replace({1: 'Home or Self-Care',
-- MAGIC                                                                 2: 'Other Facility',
-- MAGIC                                                                 3: 'SNF', 
-- MAGIC                                                                 6: 'Home Health Org', 
-- MAGIC                                                                 7: 'Left Against Medical Advice', 
-- MAGIC                                                                 20: 'Expired',
-- MAGIC                                                                 50: 'Hospice-Home',
-- MAGIC                                                                 51: 'Hospice-Medical Facility',
-- MAGIC                                                                 62: 'Another Rehab Facility',
-- MAGIC                                                                 63: 'LTC Hospital', 
-- MAGIC                                                                 65: 'Psych Facility'
-- MAGIC                                                                 })
-- MAGIC #Define facet grid order
-- MAGIC col_order = ['Home or Self-Care', 'SNF', 'Expired', 'Home Health Org', 'LTC Hospital',  'Hospice-Home', 'Hospice-Medical Facility', 'Psych Facility', 'Left Against Medical Advice', 'Other Facility', 'Another Rehab Facility']
-- MAGIC
-- MAGIC #Define palette
-- MAGIC palette = {'IDD/SDBLD-18-64': 'red',
-- MAGIC            'IDD/SDBLD-65+': 'red',
-- MAGIC            'No IDD/SDBLD-18-64': 'blue',
-- MAGIC            'No IDD/SDBLD-65+':'blue' }
-- MAGIC
-- MAGIC #Define dashes
-- MAGIC dashes = {'IDD/SDBLD-18-64': 'dashed',
-- MAGIC            'IDD/SDBLD-65+': 'solid',
-- MAGIC            'No IDD/SDBLD-18-64': 'dashed',
-- MAGIC            'No IDD/SDBLD-65+': 'solid' }
-- MAGIC
-- MAGIC
-- MAGIC #Define facet grid
-- MAGIC g = sns.FacetGrid(dis_grouped,  col = 'disc_name', hue = 'dis_age', palette=palette, col_wrap=3, col_order = col_order, sharex = False, sharey=False)
-- MAGIC
-- MAGIC #Define lineplots
-- MAGIC g.map(sns.lineplot,'year', 'proportion')
-- MAGIC
-- MAGIC #Add dashed lines
-- MAGIC for ax in g.axes.flat:
-- MAGIC   for line in ax.get_lines(): 
-- MAGIC     label = line.get_label()
-- MAGIC     if label in dashes:
-- MAGIC         line.set_linestyle(dashes[label])
-- MAGIC
-- MAGIC #Add percent formatter
-- MAGIC for ax in g.axes.flat:  
-- MAGIC   ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
-- MAGIC
-- MAGIC #Update x axis labels
-- MAGIC for ax in g.axes.flat:
-- MAGIC   years = list(range(2016, 2024, 1))
-- MAGIC   ax.set_xticks(years)
-- MAGIC   ax.set_xticklabels (years, rotation = 45)
-- MAGIC
-- MAGIC #Update table labels and titles
-- MAGIC g.set_axis_labels('Year', '% Discharged')
-- MAGIC plt.tight_layout()
-- MAGIC plt.legend(title = 'Disability Status & Age', bbox_to_anchor=(2.5, 4.25), loc='upper left', borderaxespad=0)
-- MAGIC
-- MAGIC g.set_titles('{col_name}')
-- MAGIC
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #print (dis_pd['disc_status'].drop_duplicates())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IDD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Disability and No Disability PySpark Table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dis2 = spark.sql("""
-- MAGIC   select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'IDD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_idd
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC
-- MAGIC union
-- MAGIC
-- MAGIC select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'No IDD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_idd
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert Spark Dataframe to Pandas Dataframe
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Convert Spark Dataframe to Pandas dataframe
-- MAGIC dis2_pd = dis2.toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Numerators
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis2_grouped = dis2_pd.groupby(['year','disc_status','agegrp2','dis'])['medrec_key'].nunique().reset_index(name='counts')
-- MAGIC print(dis2_grouped)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Denominator
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #calculate the total number of patients for each year by dis and age category
-- MAGIC dis2_grouped['total'] = dis2_grouped.groupby(['year','agegrp2','dis'])['counts'].transform('sum')
-- MAGIC
-- MAGIC dis2_grouped.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate proportions
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis2_grouped['proportion'] = dis2_grouped['counts']/dis2_grouped['total'] 
-- MAGIC dis2_grouped.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Line graph Facet Grid
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create Disability/Age Variable for facet legend
-- MAGIC dis2_grouped['dis_age'] = dis2_grouped['dis'] + '-' + dis2_grouped['agegrp2'] 
-- MAGIC
-- MAGIC #Create facet table labels
-- MAGIC dis2_grouped['disc_name'] = dis2_grouped['disc_status'].replace({1: 'Home or Self-Care',
-- MAGIC                                                                 2: 'Other Facility',
-- MAGIC                                                                 3: 'SNF', 
-- MAGIC                                                                 6: 'Home Health Org', 
-- MAGIC                                                                 7: 'Left Against Medical Advice', 
-- MAGIC                                                                 20: 'Expired',
-- MAGIC                                                                 50: 'Hospice-Home',
-- MAGIC                                                                 51: 'Hospice-Medical Facility',
-- MAGIC                                                                 62: 'Another Rehab Facility',
-- MAGIC                                                                 63: 'LTC Hospital', 
-- MAGIC                                                                 65: 'Psych Facility'})
-- MAGIC
-- MAGIC #Define facet grid order
-- MAGIC col_order = ['Home or Self-Care', 'SNF', 'Expired', 'Home Health Org', 'LTC Hospital',  'Hospice-Home', 'Hospice-Medical Facility', 'Psych Facility', 'Left Against Medical Advice', 'Other Facility', 'Another Rehab Facility']
-- MAGIC
-- MAGIC #Define palette
-- MAGIC palette = {'IDD-18-64': 'red',
-- MAGIC            'IDD-65+': 'red',
-- MAGIC            'No IDD-18-64': 'blue',
-- MAGIC            'No IDD-65+':'blue' }
-- MAGIC
-- MAGIC #Define dashes
-- MAGIC dashes = {'IDD-18-64': 'dashed',
-- MAGIC            'IDD-65+': 'solid',
-- MAGIC            'No IDD-18-64': 'dashed',
-- MAGIC            'No IDD-65+': 'solid' }
-- MAGIC
-- MAGIC
-- MAGIC #Define facet grid
-- MAGIC g = sns.FacetGrid(dis2_grouped,  col = 'disc_name', hue = 'dis_age', palette=palette, col_wrap=3, col_order = col_order, sharex = False, sharey=False)
-- MAGIC
-- MAGIC #Define lineplots
-- MAGIC g.map(sns.lineplot,'year', 'proportion')
-- MAGIC
-- MAGIC #Add dashed lines
-- MAGIC for ax in g.axes.flat:
-- MAGIC   for line in ax.get_lines(): 
-- MAGIC     label = line.get_label()
-- MAGIC     if label in dashes:
-- MAGIC         line.set_linestyle(dashes[label])
-- MAGIC         
-- MAGIC #Add percent formatter
-- MAGIC for ax in g.axes.flat:  
-- MAGIC   ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
-- MAGIC
-- MAGIC #Update x axis labels
-- MAGIC for ax in g.axes.flat:
-- MAGIC   years = list(range(2016, 2024, 1))
-- MAGIC   ax.set_xticks(years)
-- MAGIC   ax.set_xticklabels (years, rotation = 45)
-- MAGIC   
-- MAGIC #Update table labels and titles
-- MAGIC g.set_axis_labels('Year', '% Discharged')
-- MAGIC plt.tight_layout()
-- MAGIC plt.legend(title = 'Disability Status & Age', bbox_to_anchor=(2.5, 4.25), loc='upper left', borderaxespad=0)
-- MAGIC
-- MAGIC g.set_titles('{col_name}')
-- MAGIC
-- MAGIC
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SLDD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Disability and No Disability PySpark Dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dis3 = spark.sql("""
-- MAGIC
-- MAGIC select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'SDBLD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_sldd
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC union
-- MAGIC
-- MAGIC select distinct pat_key, medrec_key, disc_status, disc_status_desc,year(admit_date) as year,
-- MAGIC          case 
-- MAGIC           when agem between 18 and 64 then '18-64'
-- MAGIC           when agem >=65 then '65+'
-- MAGIC           else 'unknown'
-- MAGIC         end as agegrp2,
-- MAGIC         'No SDBLD' as dis
-- MAGIC
-- MAGIC       from edav_prd_cdh.cdh_premier_exploratory.xud4_m_c201623_nd_sldd
-- MAGIC     
-- MAGIC       where disc_status in ('1', '6', '3', '20', '62', '7', '2', '50', '51', '63', '65')
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert Spark Dataframe to Pandas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Convert Spark Dataframe to Pandas dataframe
-- MAGIC dis3_pd = dis3.toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Numerators

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis3_grouped = dis3_pd.groupby(['year','disc_status','agegrp2','dis'])['medrec_key'].nunique().reset_index(name='counts')
-- MAGIC print(dis3_grouped)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Denominators

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #calculate the total number of patients for each year by dis and age category
-- MAGIC dis3_grouped['total'] = dis3_grouped.groupby(['year','agegrp2','dis'])['counts'].transform('sum')
-- MAGIC
-- MAGIC dis3_grouped.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Proportions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dis3_grouped['proportion'] = dis3_grouped['counts']/dis3_grouped['total'] 
-- MAGIC dis3_grouped.head()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Line Graph Facet Grid

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Define facet legend
-- MAGIC dis3_grouped['dis_age'] = dis3_grouped['dis'] + '-' + dis3_grouped['agegrp2'] 
-- MAGIC
-- MAGIC #Create facet table labels
-- MAGIC dis3_grouped['disc_name'] = dis3_grouped['disc_status'].replace({1: 'Home or Self-Care',
-- MAGIC                                                                 2: 'Other Facility',
-- MAGIC                                                                 3: 'SNF', 
-- MAGIC                                                                 6: 'Home Health Org', 
-- MAGIC                                                                 7: 'Left Against Medical Advice', 
-- MAGIC                                                                 20: 'Expired',
-- MAGIC                                                                 50: 'Hospice-Home',
-- MAGIC                                                                 51: 'Hospice-Medical Facility',
-- MAGIC                                                                 62: 'Another Rehab Facility',
-- MAGIC                                                                 63: 'LTC Hospital', 
-- MAGIC                                                                 65: 'Psych Facility'
-- MAGIC                                                                 })
-- MAGIC
-- MAGIC #Define facet grid order
-- MAGIC col_order = ['Home or Self-Care', 'SNF', 'Expired', 'Home Health Org', 'LTC Hospital',  'Hospice-Home', 'Hospice-Medical Facility', 'Psych Facility', 'Left Against Medical Advice', 'Other Facility', 'Another Rehab Facility']
-- MAGIC
-- MAGIC #Define palette
-- MAGIC palette = {'SDBLD-18-64': 'red',
-- MAGIC            'SDBLD-65+': 'red',
-- MAGIC            'No SDBLD-18-64': 'blue',
-- MAGIC            'No SDBLD-65+':'blue' }
-- MAGIC
-- MAGIC #Define dashes
-- MAGIC dashes = {'SDBLD-18-64': 'dashed',
-- MAGIC            'SDBLD-65+': 'solid',
-- MAGIC            'No SDBLD-18-64': 'dashed',
-- MAGIC            'No SDBLD-65+': 'solid' }
-- MAGIC
-- MAGIC
-- MAGIC #Define facet grid
-- MAGIC g = sns.FacetGrid(dis3_grouped,  col = 'disc_name', hue = 'dis_age', palette=palette, col_wrap=3, col_order=col_order, sharex = False, sharey = False)
-- MAGIC
-- MAGIC #Define lineplots
-- MAGIC g.map(sns.lineplot,'year', 'proportion')
-- MAGIC
-- MAGIC #Add dashed lines
-- MAGIC for ax in g.axes.flat:
-- MAGIC   for line in ax.get_lines(): 
-- MAGIC     label = line.get_label()
-- MAGIC     if label in dashes:
-- MAGIC         line.set_linestyle(dashes[label])
-- MAGIC         
-- MAGIC #Add percent formatter
-- MAGIC for ax in g.axes.flat:  
-- MAGIC   ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0))
-- MAGIC
-- MAGIC #Update x axis labels
-- MAGIC for ax in g.axes.flat:
-- MAGIC   years = list(range(2016, 2024, 1))
-- MAGIC   ax.set_xticks(years)
-- MAGIC   ax.set_xticklabels (years, rotation = 45)
-- MAGIC
-- MAGIC #Update table labels and titles
-- MAGIC g.set_axis_labels('Year', '% Discharged')
-- MAGIC plt.tight_layout()
-- MAGIC plt.legend(title = 'Disability Status & Age', bbox_to_anchor=(2.5, 4.25), loc='upper left', borderaxespad=0)
-- MAGIC
-- MAGIC g.set_titles('{col_name}')
-- MAGIC #g.fig.suptitle('Percent of Inpatients Discharged by Year, Disability, and Age (SLDD vs. No SLDD)', y =1.05)
-- MAGIC
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Disregard

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC df = spark.table("edav_prd_cdh.cdh_premier_v2.admtype")
-- MAGIC
-- MAGIC selected_df = df.select(df.colRegex('`.*E`'))
-- MAGIC
-- MAGIC selected_df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC tableName = "edav_prd_cdh.cdh_premier_v2.admtype"
-- MAGIC
-- MAGIC df = spark.table(tableName)
-- MAGIC
-- MAGIC selected_df = df.select(df.colRegex('`.*E`'))
-- MAGIC
-- MAGIC selected_df.show()

-- COMMAND ----------

/*
%python

r = spark.sql("""
*/

--Create variables for the date a participant entered the cohort and the year, month, and quarter of admission
with cases as (
select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
case when ct_adm_month in (1, 2, 3) then 'Q1'
     when ct_adm_month in (4, 5, 6) then 'Q2'
     when ct_adm_month in (7, 8, 9) then 'Q3'
else 'Q4'
end as ct_adm_qtr
from c201623
order by medrec_key),

controls as (
  select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
case when ct_adm_month in (1, 2, 3) then 'Q1'
     when ct_adm_month in (4, 5, 6) then 'Q2'
     when ct_adm_month in (7, 8, 9) then 'Q3'
else 'Q4'
end as ct_adm_qtr
from c201623_ND
order by medrec_key),

cc as (select *, rand() as random_num
from (select * from cases union select * from controls)),

cases_rand as (select * 
               from cc
               where t = 'case'),

controls_rand as (select * 
                  from cc
                  where t = 'control'),

controls_id as (
     select 
          a.medrec_key as case_id,
          b.medrec_key as control_id,
          a.agem as case_age,
          b.agem as control_age,
          a.gender as case_gender,
          b.gender as control_gender,
          a.ct_adm_year as case_year,
          b.ct_adm_year as control_year,
          a.ct_adm_qtr as case_qtr,
          b.ct_adm_qtr as control_qtr,
          a.random_num as rand_num
     from cases_rand a
     join controls_rand b
     on a.agem = b.agem
          and a.gender = b.gender
          and a.ct_adm_year = b.ct_adm_year
          and a.ct_adm_qtr = b.ct_adm_qtr),

--select one random control 
controls_id_sorted as (
     select case_id, control_id, case_age, control_age, case_gender, control_gender, case_year, control_year, case_qtr, control_qtr, rand_num
                         from (select *, row_number() over (partition by control_id order by rand_num) as rn
                         from controls_id)
                         where rn = 1)
/*

--check for duplicates control_id values
select control_id, count(control_id)
from controls_id_sorted
group by control_id
having count(control_id) > 1
--no duplicates! Yay!

*/

--select 2 controls for every case 
select *
from (select *, row_number() over (partition by case_id order by rand_num) as rn
     from controls_id_sorted)
where rn <= 2


/*



--Match cases to controls by age, sex, year, and qtr
match as (select a.medrec_key, b.medrec_key as c_medrec_key, a.agem, a.gender, a.ct_adm_year, a.ct_adm_qtr, row_number () over (partition by a.medrec_key order by b.medrec_key) as rn
from cases a
cross join controls b
     on (a.agem = b.agem
     and a.gender = b.gender
     and a.ct_adm_year = b.ct_adm_year
     and a.ct_adm_qtr = b.ct_adm_qtr)),

controls_2 as (Select b.medrec_key AS Control_Name,
    ROW_NUMBER() OVER (PARTITION BY a.medrec_key ORDER BY RANDOM()) AS Control_RowNumber
     FROM cases a
     CROSS JOIN controls b)


-- Match cases with controls (3 controls per case)
SELECT
    a.medrec_key AS Case_Name,
    b.Control_Name
FROM cases a
JOIN controls_2 b ON a.medrec_key = b.Control_Name
WHERE b.Control_RowNumber <= 3



--find the number of matches for each person
count as (select *,count(c_medrec_key) over (partition by medrec_key) as match_count
          from match
          order by medrec_key),

--medrec_keys of all matches for cases
medrec as (select distinct medrec_key
            from match),

--limit c201623 to matched medrec_key
c201623_m as (select *
              from c201623
              where medrec_key in (select medrec_key from medrec)),

--match 1:2 
match1_2 as (select medrec_key, c_medrec_key, agem, gender, ct_adm_year, ct_adm_qtr
               from match
               where rn <= 2) 

--check for duplicates c_medrec_key values
select c_medrec_key, count(c_medrec_key)
from match1_2
group by c_medrec_key
having count(c_medrec_key) > 1



""")

r.createOrReplaceTempView('m_c201623') 

--find range of match count 

select min(match_count) as min, max(match_count) as max, median(match_count) as median
from count


--The range of matches for each participant ranges from 1 - 1914196, median is 25026

--match 1:2 
--select *, row number () over (partition by medrec_key order by c_medrec_key) as rn
--from match 

*/



-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC
-- MAGIC r = spark.sql("""
-- MAGIC
-- MAGIC --Create variables for the date a participant entered the cohort and the year, month, and quarter of admission
-- MAGIC with cases as (
-- MAGIC select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'case' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC controls as (
-- MAGIC   select medrec_key, agem, gender, admit_date, min(admit_date) over (partition by medrec_key) as ct_adm_date, 
-- MAGIC year(ct_adm_date) as ct_adm_year, month(ct_adm_date) as ct_adm_month, 'control' as t,
-- MAGIC case when ct_adm_month in (1, 2, 3) then 'Q1'
-- MAGIC      when ct_adm_month in (4, 5, 6) then 'Q2'
-- MAGIC      when ct_adm_month in (7, 8, 9) then 'Q3'
-- MAGIC else 'Q4'
-- MAGIC end as ct_adm_qtr
-- MAGIC from c201623_ND
-- MAGIC order by medrec_key),
-- MAGIC
-- MAGIC --combine case and control datasets and assign random numbers
-- MAGIC cc as (select *, rand() as random_num
-- MAGIC from (select * from cases union select * from controls)),
-- MAGIC
-- MAGIC --separate cases and controls 
-- MAGIC cases_rand as (select * 
-- MAGIC                from cc
-- MAGIC                where t = 'case'),
-- MAGIC
-- MAGIC controls_rand as (select * 
-- MAGIC                   from cc
-- MAGIC                   where t = 'control'),
-- MAGIC
-- MAGIC --join cases and controls on matching factors
-- MAGIC controls_id as (
-- MAGIC      select 
-- MAGIC           a.medrec_key as case_id,
-- MAGIC           b.medrec_key as control_id,
-- MAGIC           a.agem as case_age,
-- MAGIC           b.agem as control_age,
-- MAGIC           a.gender as case_gender,
-- MAGIC           b.gender as control_gender,
-- MAGIC           a.ct_adm_year as case_year,
-- MAGIC           b.ct_adm_year as control_year,
-- MAGIC           a.ct_adm_qtr as case_qtr,
-- MAGIC           b.ct_adm_qtr as control_qtr,
-- MAGIC           a.random_num as rand_num
-- MAGIC      from cases_rand a
-- MAGIC      join controls_rand b
-- MAGIC      on a.agem = b.agem
-- MAGIC           and a.gender = b.gender
-- MAGIC           and a.ct_adm_year = b.ct_adm_year
-- MAGIC           and a.ct_adm_qtr = b.ct_adm_qtr),
-- MAGIC
-- MAGIC --select one random control 
-- MAGIC controls_id_sorted as (
-- MAGIC      select case_id, control_id, case_age, control_age, case_gender, control_gender, case_year, control_year, case_qtr, control_qtr, rand_num
-- MAGIC                          from (select *, row_number() over (partition by control_id order by rand_num) as rn
-- MAGIC                          from controls_id)
-- MAGIC                          where rn = 1)
-- MAGIC /*
-- MAGIC
-- MAGIC --check for duplicates control_id values
-- MAGIC select control_id, count(control_id)
-- MAGIC from controls_id_sorted
-- MAGIC group by control_id
-- MAGIC having count(control_id) > 1
-- MAGIC --no duplicates! Yay!
-- MAGIC
-- MAGIC */
-- MAGIC
-- MAGIC --select 3 controls for every case 
-- MAGIC select *
-- MAGIC from (select *, row_number() over (partition by case_id order by rand_num) as rn
-- MAGIC      from controls_id_sorted)
-- MAGIC --where rn <= 3
-- MAGIC
-- MAGIC """)
-- MAGIC
-- MAGIC r.createOrReplaceTempView('matching_201623') 
-- MAGIC
-- MAGIC
-- MAGIC
