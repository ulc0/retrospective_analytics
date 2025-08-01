# Databricks notebook source
# MAGIC %md
# MAGIC ###COVID Rx Analysis with age consideration
# MAGIC Author: Erica Okene (contractor, GAP solutions) ak21@cdc.gov
# MAGIC
# MAGIC COVID Population, Definitions, and Codesets generated by Heartley Egwuogu (contractor, GAP solutions) tog0@cdc.gov for LAVA Respiratory Virus Dashboard
# MAGIC
# MAGIC FTE Support: Julia Raykin qiz8@cdc.gov
# MAGIC
# MAGIC Started on 12/20/2024

# COMMAND ----------

# DBTITLE 0,Importing  Section
from itertools import chain
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql import SparkSession, functions as F, Row
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import datetime as dt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relevant externaly created notebooks and links,imported into this notebook
# MAGIC - cdh_truveta_exploratory.tog0_truveta_encounterv2 -Encounter Table ([Notebook](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4481497015918153?o=5189663741904016)). This table is the modified Truveta encounter table that includes icu,imv and deaths. You may use the Truveta encounter table if icu,imv and deaths are not abstracted in your project. NOTE:The encounterv2 table must be rerun, to refresh table, at least weekly
# MAGIC - cdh_truveta_exploratory.ak21_truveta_MMWR_umc High-Risk/Undeylying Medical Condition ([Notebook](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4186368583545589?o=5189663741904016)). This table contains the ICD10 codes for the High Risk Conditions and Underlying Medical Conditions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applicable Rules
# MAGIC 1. All Covid patients is defined as patients with diagnosis of covid or positive labs over 65 
# MAGIC 2. Only patients with outpatient (ambulatory) encounters occuring within seasons
# MAGIC - a. Summer 2023 (2023-06-01 to 2023-09-30)
# MAGIC - b. Fall/Winter 2023 - 2024 (2023-10-01 to 2024-03-30)
# MAGIC - c. Summer 2024 (2024-06-01 to 2024-09-30)
# MAGIC 3. The following exclusions apply:
# MAGIC - a. all Patients with a COVID-19 inpatient event during the study period
# MAGIC - b. all Patients with a COVID-19 inpatient event 1 month prior or 2 weeks following an outpatient event during study period
# MAGIC - c. all Patients with a COVID-19 diagnosis 6 months prior to index COVID event
# MAGIC 4. Inpatients are defined as having conceptcode in ('1065215','1065220','1065223','1065603'): Outpatients are defined as having conceptcode in ('1065216'); emergency patients are defined as having conceptcode in ('1065217','3058549')
# MAGIC 5. Date of onset is defined as index date i.e. the first time occurence of diagnosis or positive lab
# MAGIC 7. Demographic vars including age, sex, HHS region are identified at index date.
# MAGIC 8. All covid patients are counted once in the year-month and season of the index date
# MAGIC 9. Covid medications are considered as administration or dispensation of covid medications including paxlovid or molnupiravir or remdesivir. 
# MAGIC 10. Covid medications administration or dispensation window is set to  [0,14] of index date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Covid Population Defined
# MAGIC Patients are included in this analysis based on encounters occuring in the study period with the COVID codes below (covid diagnosis or lab)

# COMMAND ----------

# DBTITLE 1,Covid Codes
# covid is defined as having positive labs or covid diagnosis

coviddg=["U07.1"]

covidlab=["95521-1", "94510-5", "94311-8", "94312-6", "95522-9", "94644-2", "94511-3", "94646-7", "94645-9", "94745-7",  "94746-5", "94819-0", "94642-6","94643-4", "94313-4", "94509-7", "95423-0", "94760-6", "95409-9", "94533-7", "94756-4", "94757-2", "95425-5",  "94766-3", "94316-7", "94307-6","94308-4", "94559-2", "95824-9", "94639-2", "94534-5", "94314-2", "94565-9", "94759-8", "95406-5", "95608-6",  "94500-6", "95424-8", "94845-5","94822-4", "94660-8", "94309-2", "94640-0", "95609-4", "94767-1", "94641-8", "94764-8", "94310-0", "94758-0",  "95823-1", "94765-5","94315-9", "94502-2", "94647-5", "94532-9", "95970-0", "96091-4", "94306-8", "96120-1", "96121-9", "96122-7",  "96123-5", "96448-6", "96741-4","96751-3", "96752-1", "96763-8", "96764-6", "96765-3", "96766-1", "96797-6", "96829-7", "96894-1", "96895-8",   "96896-6", "96897-4","96898-2", "96899-0", "96900-6", "96957-6", "96958-4", "96986-5", "95826-4", "96094-8", "97098-8",  "98132-4",  "98494-8", "97104-4", "98131-6","98493-0", "98062-3", "96756-2", "96757-0",	"98080-5", "94531-1", "99314-7", "100156-9", "95209-3", "94558-4",	"96119-3", "97097-0", "94661-6", "95825-6", "94762-2", "94769-7", "94562-6", "94768-9", "95427-1", "94720-0",  "95125-1",	"94761-4",	"94563-4", "94507-1",	"95429-7", "94505-5", "94547-7", "95542-7",	"95416-4", "94564-2", "94508-9", "95428-9",	 "94506-3",	"95411-5",	"95410-7",	"96603-6",	"96742-2", "96118-5", "98733-9", "98069-8",	"98732-1", "98734-7", "94504-8", "94503-0",	 "99596-9", "99597-7",	"95971-8",	"95972-6", "95973-4", "96755-4", "98846-9", "98847-7",	"95974-2", "94763-0", "99771-8", "99774-2",  "99773-4", "99772-6"]

colc=StructType([StructField("conceptcode",StringType(),True)])
covid=list(chain(coviddg,covidlab))
coviddf = spark.createDataFrame(covid,colc).createOrReplaceTempView("conceptcodecovid_T")

cdiag = spark.createDataFrame(coviddg, colc).createOrReplaceTempView("conceptcodediag_T")

# COMMAND ----------

# DBTITLE 1,Inclusion and Exclusion
#Positive and invalid labs
#Identify and include patientts with positive labs while excluding erroneous invalid labs
i=spark.sql("""select conceptid from cdh_truveta.concept where conceptcode in ("1065667","1065692","260373001","720735008","10828004","52101004")""").createOrReplaceTempView("positivelab")

inv=spark.sql("""select conceptid from cdh_truveta.concept where conceptcode in ("1065712","1065714","1065719","1065717")""").createOrReplaceTempView("invalidlab")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdh_truveta_exploratory.tog0_truveta_encounterv2

# COMMAND ----------

# MAGIC %md
# MAGIC #### COVID Core Population
# MAGIC Definition: All  patients in the tog0_truveta_encounterv2 with a COVID diagnosis or positive lab.
# MAGIC
# MAGIC - The General COVID population includes patients with at least 1 COVID positive lab or diagnosis encounter within Seasons of interest from tog0_truveta_encounterv2. 
# MAGIC - HHS Region, Race/Ethnicity, Gender, 14-day Encounter window  established in gp
# MAGIC - Inpatient/outpatient flag established in tog0_truveta_encounterv2 as clr
# MAGIC - Patients with inpatient encounters  are excluded in the table below based on inpatient encounter date (1 month prior or 2 weeks after index)

# COMMAND ----------

#Covid Population
#Covid population: 
cp=spark.sql("""
  with conid as (select conceptid 
                 from cdh_truveta.concept
                 where 1=1
                 and conceptcode in (select conceptcode from conceptcodecovid_t) --Generate Covid pop. using Diagnosis and Lab 
                 ),

         

   cond as (select personid,encounterid, recordeddatetime as dt
            from cdh_truveta.condition
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.conditioncodeconceptmap where codeconceptid in (select conceptid from conid))
            ),

    medreq as (select personid,encounterid,authoredondatetime as dt 
            from cdh_truveta.medicationrequest
            where 1=1
           and CodeConceptMapId in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from conid))
            ),

 medadm as (select personid,encounterid,recordeddatetime as dt 
            from cdh_truveta.medicationadministration
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from conid))
            ),

  meddisp as (select personid,"" as encounterid, recordeddatetime as dt
            from cdh_truveta.medicationdispense
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from conid))
            ),

  lab as (select personid,encounterid,effectivedatetime as dt
          from cdh_truveta.labresult l
         
          where 1=1
          and codeconceptmapid in (select id from cdh_truveta.labresultcodeconceptmap where codeconceptid in (select conceptid from conid))
          and l.NormalizedValueConceptId in (select conceptid from positivelab)
          and l.NormalizedValueConceptId not in (select conceptid from invalidlab)
         ),

  obs as (select personid,encounterid, recordeddatetime as dt
            from cdh_truveta.observation
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.observationcodeconceptmap where codeconceptid in (select conceptid from conid))
        ),
  
  proc as (select personid,encounterid, recordeddatetime as dt
          from cdh_truveta.procedure
          where 1=1
          and codeconceptmapid in (select id from cdh_truveta.procedurecodeconceptmap where codeconceptid in (select conceptid from conid))
        )

                --Identify and union all patients found in core tables and select encounterid which will be used to 
                --join to encounter table in order to identify details of each patients encounter including start 
                --and end datetime,admission status(out,inp,ed) discharge status etc
                select distinct encounterid, 1 as covid
                from(select personid,encounterid,dt from cond union
                     select personid,encounterid,dt from medreq union 
                     select personid,encounterid,dt from medadm union  --no inpact
                     select personid,encounterid,dt from meddisp union  --no inpact
                     select personid,encounterid,dt from lab union
                     select personid,encounterid,dt from obs union
                     select personid,encounterid,dt from proc --no inpact
                 ) where encounterid is not null
           
""")
cp.write.mode("overwrite").saveAsTable("cdh_truveta_exploratory.ak21_truveta_covid")
#covid=1 are patients and thier encounters associated with covid diagnosis or covid positive labs

# COMMAND ----------

# DBTITLE 1,COVID General Population
gp=spark.sql("""
with genc as (select 
            en.personid,
            en.id,
            en.startdatetime,
            en.enddatetime,
            en.clr,
            en.discharge,
            case  --UPDATE 1
               when cast(startdatetime as date) between '2023-06-01' and '2023-09-30' then "Summer 2023" 
               when cast(startdatetime as date) between '2023-10-01' and '2024-03-30' then "Fall/Winter 2023-2024"
               when cast(startdatetime as date) between '2024-06-01' and '2024-09-30' then "Summer 2024"
            end as season,
            coalesce(cp.covid,0) as encovid,
            max(coalesce(cp.covid,0)) over(partition by en.personid) as covid  --flag only patients with at least 1 covid encounter
            
            from (select distinct personid,id,startdatetime,enddatetime,clr,discharge from cdh_truveta_exploratory.ak21_truveta_encounterv2
                  where 1=1
                  and id is not null
                  and clr in ('out','inp') 
                  and startdatetime between '2023-06-01' and '2024-09-30' ) en   --UPDATE 2 Encounters of interest
                  left join cdh_truveta_exploratory.ak21_truveta_covid cp on en.id=cp.encounterid
                   
             

          ),

    tp as (select 
            personid,covid,encovid,id,startdatetime,clr,
            season,
            first_value(startdatetime) over(partition by personid,season order by encovid desc, startdatetime) as indexdate,
            datediff(startdatetime,first_value(startdatetime) over(partition by personid,season order by encovid desc, startdatetime)) as encwindow, --UPDATE 3 encounterwindow variable
            max(encovid) over(partition by personid,season) as validseason, --UPDATE 4 select valid season,
            max(case when clr='inp' then 1 else 0 end) over(partition by personid,season) as inpflag,
            max(case when clr='out' then 1 else 0 end) over(partition by personid,season) as outflag


            from genc
            where 1=1
            and covid = 1
            
          ),
region as (select personid,
           case 
            when st.conceptname in ('Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont') then "Region 1-Boston"
            when st.conceptname in ('New Jersey', 'New York', 'Puerto Rico','Virgin Islands') then "Region 2-New York"
            when st.conceptname in ('Delaware', 'District of Columbia', 'Maryland', 'Pennsylvania', 'Virginia', 'West Virginia') then "Region 3-Philadelphia"
            when st.conceptname in ('Alabama', 'Florida', 'Georgia', 'Kentucky', 'Mississippi', 'North Carolina', 'South Carolina','Tennessee') then "Region 4-Atlanta"
            when st.conceptname in ('Illinois', 'Indiana', 'Michigan', 'Minnesota', 'Ohio', 'Wisconsin') then "Region 5-Chicago"
            when st.conceptname in ('Arkansas', 'Louisiana', 'New Mexico', 'Oklahoma','Texas') then "Region 6-Dallas"
            when st.conceptname in ('Iowa', 'Kansas', 'Missouri','Nebraska') then "Region 7-Kansas City"
            when st.conceptname in ('Colorado', 'Montana', 'North Dakota', 'South Dakota', 'Utah', 'Wyoming') then "Region 8-Denver"
            when st.conceptname in ('Arizona', 'California', 'Hawaii', 'Nevada', 'American Samoa', 'Commonwealth of the Northern Mariana Islands', 'Federated States of Micronesia', 'Guam', 'Marshall Islands', 'Republic of Palau') then "Region 9-San Francisco"
            when st.conceptname in ('Alaska', 'Idaho', 'Oregon','Washington') then "Region 10-Seattle"
         end as HHSRegion
         from cdh_truveta.personlocation pl                                    
         left join cdh_truveta.location lc on  pl.locationid=lc.id
         left join cdh_truveta.concept st on st.conceptid=lc.stateorprovinceconceptid
         ),
--Gender, DOB, Race and Ethincity updated according to program request
eth_gen as (select  e.id,
            case 
            when r.raceconceptid in ('1066466')and e.ethnicityconceptid in ('1065401') then 'AI/AN, non-Hispanic'
            when r.raceconceptid in ('1067294')and e.ethnicityconceptid in ('1065401') then 'Asian, non-Hispanic'
            when r.raceconceptid in ('1067319')and e.ethnicityconceptid in ('1065401') then 'Black, non-Hispanic'
            when r.raceconceptid in ('1067338')and e.ethnicityconceptid in ('1065401') then 'NH/PI, non-Hispanic'
            when r.raceconceptid in ('1067364')and e.ethnicityconceptid in ('1065401') then 'White, non-Hispanic'
            when r.raceconceptid in ('1067388')and e.ethnicityconceptid in ('1065401') then 'Multiple, non-Hispanic'
  
           when r.raceconceptid in ('1066466')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'AI/AN, Hispanic or Latino'
           when r.raceconceptid in ('1067294')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'Asian, Hispanic or Latino'
           when r.raceconceptid in ('1067319')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'Black, Hispanic or Latino'
           when r.raceconceptid in ('1067338')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'NH/PI, Hispanic or Latino'
           when r.raceconceptid in ('1067364')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'White, Hispanic or Latino'
           when r.raceconceptid in ('1067388')and e.ethnicityconceptid in ('1065359','1065398','1065371','1065370','1065399','1065376') then 'Multiple, Hispanic or Latino'

           else 'Unknown/Other'
          end as Race_Ethnicity,
          sx.conceptname as gender,
          date_add(date_trunc('MM', e.BirthDateTime),14) as dob  --Date of Birth Added and updated Vevek D

          from cdh_truveta.person e
          left join cdh_truveta.concept ec on ec.conceptid = e.ethnicityconceptid
          left join cdh_truveta.personrace r on r.personid = e.id
          left join cdh_truveta.concept rc on rc.conceptid = r.raceconceptid
          left join cdh_truveta.concept sx on sx.conceptid=e.genderconceptid
         
          )
         
       select tp.personid,tp.id,dob,race_ethnicity,gender,covid,encovid,clr,season,startdatetime,
              indexdate,encwindow
              --determine index events associated with a covid diagnosis, sort startdates within each season, covid dx encounters first

  
      from tp
      left join eth_gen on eth_gen.id=tp.personid
      where 1=1 
      and startdatetime between add_months(indexdate, -6) and indexdate --UPDATE 7* exclude patients with covid encounter up to  6months prior to index date
      and NOT ((startdatetime between add_months(indexdate, -1) and indexdate and inpflag = 1) 
         or (startdatetime between indexdate and date_add(indexdate, 14) and inpflag = 1))--Update 8* exclude patients with inpatient encounter 1 month prior or 2 weeks following index date
      and validseason=1 --UPDATE 5
      and encwindow between 0 and 14  --UPDATE 6
      order by personid,season,encovid desc,startdatetime
        
""")
gp.write.mode("overwrite").saveAsTable("cdh_truveta_exploratory.ak21_truveta_covidgp_enc")

#Common Table Expression (CTE):
  #genc= generate encounter events
  #tp= all encounter population charctracteristics including covid=1 and covid=0
  #race_ethnicity= race and ethnicity combined
  #eth_gen= race and ethnicity combined , and gender
  
  #encovid --DISPLAYS ONLY ENCOUNTTERS ASSOCIATED WITH COVID DGX OR +LAB(s) USE TO ID SETTINGS IN COVID POPULATION
  #covid -FLAGS ALL PATIENT RECORDS IN A PANEL AS 1 WHEN AT LEAST ONE ENCOUNTER IS ASSOCIATED WITH COVID DGX OR +LAB(S) ITS USED TO IDENTIFYSETTINGS IN GENERAL POPULATION
  

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct personid) as count from cdh_truveta_exploratory.tog0_truveta_encounterv2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct personid) as count from cdh_truveta_exploratory.ak21_truveta_encounterv2

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists cdh_truveta_exploratory.ak21_truveta_covidgp_enc

# COMMAND ----------

# MAGIC %md
# MAGIC M table derived in LAVA, modified in this notebook table altered to include only medications dispensed  or administered after 2023/06/01
# MAGIC
# MAGIC
# MAGIC Seasons were updated to reflect Project Proposal
# MAGIC - summer 2024 (June 1 – Sept 30, 2024), 
# MAGIC - fall/winter 2023-2024 (Oct 1, 2023 – March 1, 2024), 
# MAGIC - summer 2023 (June 1- Sept 30, 2023). 
# MAGIC We will exclude all cases outside of the above three dates in our analysis.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Covid Medications
#define medications of interest by Conceptcodes: Paxlovid,Molnupiravir and Remdesivir

pax=["2587899", "2587898", "2587892", "2587897", "2587893", "2587896", "2587894", "2587895", "2599543", "00069108530", "00069108506", "00069034506", "00069034530", "00069110104", "00069110120", "00069208501", "00069208502"]

mol=["2587901", "2587906", "2587902", "2587905", "2587903", "2587904", "2603735", "2603736", "2603739", "2603740", "2603737", "2603738", "00006505507", "00006505506"]

rem=['2284718','2284957','2284958','2284959','2284960','2367757','2367758','2367759', '2395498','2395499','2395500','2395501','2395502','2395503','2395504','2395505','61958290101','61958290102','870600003','61958290101','61958290102','61958290201','61958290202','J0248','61958999899','61958999999','XW033E5','XW043E5']

# meddis=list(chain(pax,mol,rem))
col=StructType([StructField("conceptcode",StringType(),True)])
spark.createDataFrame(pax, col).createOrReplaceTempView("conceptcodemedpax_T")
spark.createDataFrame(mol, col).createOrReplaceTempView("conceptcodemedmol_T")
spark.createDataFrame(rem, col).createOrReplaceTempView("conceptcodemedrem_T")

#pax=paxlovid
#mol=molnupiravir
#rem=remdesivir

# COMMAND ----------

# DBTITLE 0,Medication Dispensed or Administered

m=spark.sql("""
  --Patients with Medication Dispense/Or Medication Adminsitration (Pax,Mol,Rem)

  ---------------------Pax
  with dconidpax as (select conceptid 
                     from cdh_truveta.concept
                     where 1=1
                     and conceptcode in (select conceptcode from conceptcodemedpax_t) --Generate pax conceptid
                    ),
  --Generate Set of patients who received pax by dispensation only
adpax as (select distinct personid,dispensedatetime as meddate, 'pax' as meds
            from cdh_truveta.medicationdispense
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidpax))
            and dispensedatetime between '2023-06-01' and getdate()          
            union

            select distinct personid, recordeddatetime as meddate, 'pax' as meds
            from cdh_truveta.medicationadministration
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidpax))
            and recordeddatetime between '2023-06-01' and getdate()        
           ),    --End Medication Dispense for pax
---------------------------Mol
       dconidmol as (select conceptid 
                     from cdh_truveta.concept
                     where 1=1
                     and conceptcode in (select conceptcode from conceptcodemedmol_t) --Generate mol meds conceptid
                    ),

  --Generate Set of patients who received Mol by dispensation only
admol as (select distinct personid,dispensedatetime as meddate, 'mol' as meds
            from cdh_truveta.medicationdispense
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidmol))
            and dispensedatetime between '2023-06-01' and getdate()
            union

            select distinct personid, recordeddatetime as meddate, 'mol' as meds
            from cdh_truveta.medicationadministration
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidpax))
            and recordeddatetime between '2023-06-01' and getdate()        
           ),    --End Medication Dispense for mol

-----------------------------Rem
      dconidrem as (select conceptid 
                     from cdh_truveta.concept
                     where 1=1
                     and conceptcode in (select conceptcode from conceptcodemedrem_t) --Generate rem meds conceptid
                    ),
    --Generate Set of patients who received Rem by dispensation only
adrem as (select distinct personid,dispensedatetime as meddate, 'rem' as meds
            from cdh_truveta.medicationdispense
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidrem))
            and dispensedatetime between '2023-06-01' and getdate()            
            union

            select distinct personid, recordeddatetime as meddate, 'pax' as meds
            from cdh_truveta.medicationadministration
            where 1=1
            and codeconceptmapid in (select id from cdh_truveta.medicationcodeconceptmap where codeconceptid in (select conceptid from dconidpax))
            and recordeddatetime between '2023-06-01' and getdate()        
           )  

             --End Medication Dispense for rem

              select distinct *,
              case  
               when cast(meddate as date) between '2023-06-01' and '2023-09-30' then "Summer 2023" 
               when cast(meddate as date) between '2023-10-01' and '2024-03-30' then "Fall/Winter 2023-2024"
               when cast(meddate as date) between '2024-06-01' and '2024-09-30' then "Summer 2024"
              end as medseason
             from (select * from adpax union select * from admol union select * from adrem) 
             where meddate is not null

""").write.mode("overwrite").saveAsTable("cdh_truveta_exploratory.ak21_truveta_lavacovidtp_meds")  

#DEFFINITIONS OF VARIABLLES
#personid=Unique patient identifier
#pax=Paxlovid flag
#mol=Molnupiravir flag
#rem=Remdesivir flag
#meddate=Date of medication dispensation or administration
#Common Tabel Expression (CTE):
  #dconidpax =conceptid for paxlovid
  #dconidmol =conceptid for molnupiravir
  #dconidrem =conceptid for remdesivir
  #adpax=patients who received pax by administration and dispensation
  #admol=patients who received mol by administration and dispensation
  #adrem=patients who received rem by administration and dispensation


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Target Population Table with conditions
# MAGIC ####Population characteristics addressed here: 
# MAGIC - Age: include only >=65
# MAGIC - Age groups established
# MAGIC
# MAGIC ####Follow up time:
# MAGIC - Treatment population include med dispense or admin within 14 days
# MAGIC #### Outcomes addressed:
# MAGIC - Time to treatment flag established
# MAGIC
# MAGIC #### Underlying medical conditions:
# MAGIC - [COVID UMC list and associated codes](https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/4186368583545589?o=5189663741904016)
# MAGIC - time to treatment flag established
# MAGIC - vaccination flag established (does not include vax within 2 months of index event)

# COMMAND ----------

p=spark.sql("""
with covidpop as
        (select * 
         from(select distinct personid,covid,encovid,encwindow,race_ethnicity,gender,startdatetime,clr,season,
              min(startdatetime) over(partition by tp.personid,season) as seasonindexdate,
              --concat(substr(min(StartDatetime) over(partition by tp.personid,season),1,4),
              --substr(min(StartDatetime) over(partition by tp.personid,season),6,2)) as seasonindexym,
--Population Requirement (Denom): 
--Follow-up time <= 14 days, captured in GP
--Age >= 65
--Outpatient only
--COVID Dx and Lab Only. Exclude COVID Rx without diagnosis or lab., Captured in GP
              round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) as age,
              
              case            
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) between 65 and 69 then "65-69yr"              
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) between 70 and 74 then "70-74yr"
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) between 75 and 79 then "75-79yr"
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) between 80 and 84 then "80-84yr"
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) between 85 and 89 then "85-89yr"                         
              when round(datediff(min(StartDatetime) over(partition by tp.personid,season),dob)/365) >= 90 then "90+yr"
              end agesi 
             from cdh_truveta_exploratory.ak21_truveta_covidgp_enc as tp 
             )
          where age >= 65 --age over 65
          and clr = "out" --outpatient only 
       
        ),


--Population Requirement: (Num) COVID diagnosis or lab only. Exclude COVID Rx without diagnosis or lab. 
      --Treatment population
      trt as (select distinct personid,meddate,medseason,meds 
             from cdh_truveta_exploratory.ak21_truveta_lavacovidtp_meds
             where medseason is not null
             ),
            
     umcon as (select distinct personid, dt,umc
               from cdh_truveta_exploratory.ak21_truveta_MMWR_rx_cmc 
               --where 1=1
               where umc IS NOT NULL
               )

               
          --create time to treatment flag
          select distinct i.personid,encovid,covid,race_ethnicity,gender,clr,age,agesi,season,seasonindexdate,startdatetime,umc,
          
          meds,
          case 
          when datediff(meddate,seasonindexdate) between -1 and 1 then "+/- 1 day"
          when datediff(meddate,seasonindexdate) between -2 and 2 then "+/- 2 days"
          when datediff(meddate,seasonindexdate) between -3 and 3 then "+/- 3 days"
          when datediff(meddate,seasonindexdate) between -4 and 4 then "+/- 4 days"
          when datediff(meddate,seasonindexdate) between -5 and 5 then "+/- 5 days"
          when datediff(meddate,seasonindexdate) between -6 and 6 then "+/- 6 days"
          when datediff(meddate,seasonindexdate) between -7 and 7 then "+/- 7 days"
          when meds is null then "no treatment"             
          else "> 7 days" end as ttRxflag,      
          max(case when (meds in ('rem','pax','mol') AND datediff(meddate,seasonindexdate) between 0 and 14) then 1 else 0 end) over(partition by i.personid,season) as medflag, 

          max(case when umc='sterrx' and round(datediff(seasonindexdate,dt)/365)<=1 then 1 else 0 end) over(partition by i.personid,season)  as 12msterrxflag,
          --number of conditions?
          --index event lab or cov positive
          max(case when datediff(meddate,seasonindexdate) <= 7 then 1 else 0 end) over(partition by i.personid,season) as ttrx7orlessflag,
          max(case when datediff(meddate, seasonindexdate) > 7 then 1 else 0 end) over(partition by i.personid,season) as ttrxgreater7flag, 
          max(case when (umc = "vax" and NOT (dt between add_months(seasonindexdate, -2) and seasonindexdate)) then 1 else 0 end) over(partition by i.personid,season) as vaxflag, --update 9, ignore vax codes within 2 months of seasonindexdate   
          max(case when umc='vax' and (dt between add_months(seasonindexdate, -6) and add_months(seasonindexdate, -2)) then 1 else 0 end) over(partition by i.personid,season) as 6mvaxflag
          --count(umc) over(partition by i.personid, season) as num_umc
          

          from covidpop i 
          left join trt t on i.personid=t.personid and t.medseason=i.season
          left join umcon u on i.personid=u.personid
            and round(datediff(seasonindexdate, dt)/365)<=5 --conditions occuring within 5 years of seasonindexdate
          order by i.personid

""").write.mode('overwrite').saveAsTable('cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop_enc')
#.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop')  

# Use this Target Population Table to generate solutions to specific requests as shown in the counts below
#Note: imcomp=Immnunosuppresed; preg=Pregnancy; cvd=Cardiovascular Disease; hem=Haematological; ren=Renal; pul=Pulmonary; hep=Hepatic; neuro=Neurological; met=Metabolic; ppatum=PostPartum
#tp='covid' or tp="cf" -selects from umc table only covid high risk and underlying medical conditions. cf=high risk and underlying medical conditions common to flu and covid
#clr = admssion type ("inpatient", "outpatient", "emergency")
# NOTE: Must set the following flags to meet the criteria:encovid=1 and win_1to14flagseason=1. 
# NOTE: Set the following flag as desired: inpatient(inpflag=1), outpatient(outflag=1) and ed(edflag=1)



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct personid) as count from cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop_enc

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop_enc

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists cdh_truveta_exploratory.ak21_truveta_MMWR_rx_tpop