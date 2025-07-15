-- Databricks notebook source
-- COMMAND ----------

create or replace temp view charges as
select a.pat_key, a.std_chg_code, b.clin_sum_code, b.clin_dtl_code
from patbill as a
inner join chgmstr as b 
on a.std_chg_code=b.std_chg_code
where clin_sum_code = "110102" or clin_dtl_code ="410412946570007"

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW IN_RESP as
SELECT DISTINCT A.PAT_KEY, MEDREC_KEY, ADMIT_DATE, I_O_IND, PAT_TYPE, a.DISC_STATUS, AGE, GENDER, RACE, HISPANIC_IND, a.PROV_ID, ICD_CODE, prov_region, prov_division, a.year, c.URBAN_RURAL, 
case when ICD_CODE  in ('U07.1', 'U07.2', 'J12.81') then 1 else 0 end as COVID,
case when ICD_CODE  in ('B97.4', 'J12.1', 'J20.5', 'J21.0') then 1 else 0 end as RSV,
case when ICD_CODE  in ('J09', 'J09.X', 'J09.X1', 'J09.X2', 'J09.X3', 'J09.X9', 'J10.1', 'J10.2', 'J10.81', 'J10.82', 'J10.83', 'J10.89', 'J11.1', 'J11.2', 'J11.8', 'J11.81', 'J11.82', 'J11.83', 'J11.89') then 1 else 0 end as FLU,
case when A.disc_status in (20,40,41,42) then 1 else 0 end as EXPIRED,
case when clin_sum_code =110102 then 1 else 0  end as ICU,
case when clin_dtl_code =410412946570007 then 1 else 0 end as Vent,
case 
  when AGE<=4 then '00-04' 
  when AGE>=5 and AGE<=17 then '05-17' 
  when AGE>=18 and AGE<=49 then '18-49' 
  when AGE>=50 and AGE<=64 then '50-64' 
  when AGE>=65 then '65+' 
  end as agecat,
Case
  when hispanic_ind='Y' then "Hispanic"
  when hispanic_ind='U' then 'Unknown'
  when RACE='W' and HISPANIC_IND='N' then 'nH White'
  when RACE='B' and HISPANIC_IND='N' then 'nH Black' 
  when RACE='A' and HISPANIC_IND='N' then 'nH Asian'
  when RACE='O' and HISPANIC_IND='N' then 'nH Other'
  when RACE='U' and HISPANIC_IND='N' then 'Unknown'
  end as raceeth
FROM patdemo AS a INNER JOIN paticd_diag AS b
ON a.pat_key = b.pat_key
left join providers as c 
on a.prov_id = c.prov_id 
left join disstat as d
on a.disc_status = d.disc_status
left join charges as e
on a.pat_key = e.pat_key
where  ICD_PRI_SEC in ('P','S') AND ADMIT_DATE >='2024-01-01' and PAT_TYPE=08 and I_O_IND='I' and ICD_CODE in(/*COVID*/ 'U07.1', 'U07.2', 'J12.81' /*RSV*/ 'B97.4', 'J12.1', 'J20.5', 'J21.0'  /*FLU*/ 'J09', 'J09.X', 'J09.X1', 'J09.X2', 'J09.X3', 'J09.X9', 'J10.1', 'J10.2', 'J10.81', 'J10.82', 'J10.83', 'J10.89', 'J11.1', 'J11.2', 'J11.8', 'J11.81', 'J11.82', 'J11.83', 'J11.89' )
;
CREATE OR REPLACE TEMP VIEW IN_RESP_DUP as
SELECT DISTINCT *
from IN_RESP

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW IN_RESP_1 as
SELECT DISTINCT PAT_KEY, MAX(ICU) AS ICU, MAX(VENT) AS VENT
FROM IN_RESP
AS A GROUP BY PAT_KEY



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW IN_RESP_DUP_1 as
SELECT DISTINCT A.PAT_KEY, AGE, GENDER, RACE, HISPANIC_IND, PROV_REGION, PROV_DIVISION, URBAN_RURAL, COVID, FLU, RSV, AGECAT, RACEETH, EXPIRED, B.ICU, B.VENT, to_char(ADMIT_DATE, 'yyyy-MM') AS ADM_MON
FROM IN_RESP AS A 
LEFT JOIN IN_RESP_1 AS B
ON A.PAT_KEY = B.PAT_KEY

-- COMMAND ----------

SELECT * FROM IN_RESP_DUP_1

-- COMMAND ----------

-- MAGIC %md # OVERALLS BY CONDITION
-- MAGIC

-- COMMAND ----------

select COVID, ADM_MON, count(distinct pat_key) as COVID 
from IN_RESP_DUP_1
WHERE COVID=1 AND AGE <=17
group by COVID, ADM_MON

-- COMMAND ----------

select COVID, ADM_MON, count(distinct pat_key) as COVID 
from IN_RESP_DUP_1
WHERE COVID=1 AND AGE >=18
group by COVID, ADM_MON

-- COMMAND ----------

select COVID, ADM_MON, count(distinct pat_key) as COVID_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1 AND AGE <=17
group by COVID, ADM_MON


-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as COVID_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select COVID, ADM_MON, count(distinct pat_key) as COVID_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1 AND AGE <=17
group by COVID, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as COVID_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select COVID, ADM_MON, count(distinct pat_key) as COVID_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1 AND AGE <=17
group by COVID, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as COVID_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select RSV, ADM_MON, count(distinct pat_key) as RSV
from IN_RESP_DUP_1
WHERE RSV=1 AND AGE <=17
group by RSV, ADM_MON

-- COMMAND ----------

select RSV, ADM_MON, count(distinct pat_key) as RSV
from IN_RESP_DUP_1
WHERE RSV=1 AND AGE >=18
group by RSV, ADM_MON

-- COMMAND ----------

select RSV, ADM_MON, count(distinct pat_key) as RSV_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1 AND AGE <=17
group by RSV, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as RSV_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select RSV, ADM_MON, count(distinct pat_key) as RSV_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND RSV=1 AND AGE <=17
group by RSV, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as RSV_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND RSV=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select RSV, ADM_MON, count(distinct pat_key) as RSV_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1 AND AGE <=17
group by RSV, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as RSV_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select FLU, ADM_MON, count(distinct pat_key) as FLU
from IN_RESP_DUP_1
WHERE FLU=1 AND AGE <=17
group by FLU, ADM_MON

-- COMMAND ----------

select FLU, ADM_MON, count(distinct pat_key) as FLU
from IN_RESP_DUP_1
WHERE FLU=1 AND AGE >=18
group by FLU, ADM_MON

-- COMMAND ----------

select FLU, ADM_MON, count(distinct pat_key) as FLU_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1 AND AGE <=17
group by FLU, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as FLU_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select FLU, ADM_MON, count(distinct pat_key) as FLU_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1 AND AGE <=17
group by FLU, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as FLU_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

select FLU, ADM_MON, count(distinct pat_key) as FLU_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1 AND AGE <=17
group by FLU, ADM_MON

-- COMMAND ----------

select ADM_MON, count(distinct pat_key) as FLU_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1 AND AGE >=18
group by ADM_MON

-- COMMAND ----------

-- MAGIC %md # OVERALL BY GENDER
-- MAGIC

-- COMMAND ----------

select ADM_MON, GENDER, count(distinct pat_key) as COMBINED_SEX
from IN_RESP_DUP_1
group by ADM_MON, GENDER

-- COMMAND ----------

select ADM_MON, GENDER, count(distinct pat_key) as COMBINED_SEX_ICU
from IN_RESP_DUP_1
WHERE ICU=1
group by ADM_MON, GENDER

-- COMMAND ----------

select ADM_MON, GENDER, count(distinct pat_key) as COMBINED_SEX_VENT
from IN_RESP_DUP_1
WHERE VENT=1
group by ADM_MON, GENDER

-- COMMAND ----------

select ADM_MON, GENDER, count(distinct pat_key) as COMBINED_SEX_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1
group by ADM_MON, GENDER

-- COMMAND ----------

-- MAGIC %md # OVERAL BY AGE
-- MAGIC

-- COMMAND ----------

select ADM_MON, AGECAT, count(distinct pat_key) as COMBINED_AGE
from IN_RESP_DUP_1
group by ADM_MON, AGECAT

-- COMMAND ----------

select ADM_MON, AGECAT, count(distinct pat_key) as COMBINED_AGE_ICU
from IN_RESP_DUP_1
WHERE ICU=1
group by ADM_MON, AGECAT

-- COMMAND ----------

select ADM_MON, AGECAT, count(distinct pat_key) as COMBINED_AGE_VENT
from IN_RESP_DUP_1
WHERE VENT=1
group by ADM_MON, AGECAT

-- COMMAND ----------

select ADM_MON, AGECAT, count(distinct pat_key) as COMBINED_AGE_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1
group by ADM_MON, AGECAT

-- COMMAND ----------

-- MAGIC %md # OVERALL BY RACE
-- MAGIC

-- COMMAND ----------

select ADM_MON, RACEETH, count(distinct pat_key) as COMBINED_RACE
from IN_RESP_DUP_1
group by ADM_MON, RACEETH

-- COMMAND ----------

select ADM_MON, RACEETH, count(distinct pat_key) as COMBINED_RACE_ICU
from IN_RESP_DUP_1
WHERE ICU=1
group by ADM_MON, RACEETH

-- COMMAND ----------

select ADM_MON, RACEETH, count(distinct pat_key) as COMBINED_RACE_VENT
from IN_RESP_DUP_1
WHERE VENT=1
group by ADM_MON, RACEETH

-- COMMAND ----------

select ADM_MON, RACEETH, count(distinct pat_key) as COMBINED_RACE_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1
group by ADM_MON, RACEETH

-- COMMAND ----------

-- MAGIC %md # CONDITION BY GENDER
-- MAGIC

-- COMMAND ----------

select COVID, ADM_MON, GENDER, count(distinct pat_key) as COVID_GENDER 
from IN_RESP_DUP_1
WHERE COVID=1
group by COVID, ADM_MON, GENDER

-- COMMAND ----------

select COVID, ADM_MON, GENDER, count(distinct pat_key) as COVID_GENDER_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1
group by COVID, ADM_MON, GENDER

-- COMMAND ----------

select COVID, ADM_MON, GENDER, count(distinct pat_key) as COVID_GENDER_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1
group by COVID, ADM_MON, GENDER

-- COMMAND ----------

select COVID, ADM_MON, GENDER, count(distinct pat_key) as COVID_GENDER_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1
group by COVID, ADM_MON, GENDER

-- COMMAND ----------

select RSV, ADM_MON, GENDER, count(distinct pat_key) as RSV_GENDER 
from IN_RESP_DUP_1
WHERE RSV=1
group by RSV, ADM_MON, GENDER

-- COMMAND ----------

select RSV, ADM_MON, GENDER, count(distinct pat_key) as RSV_GENDER_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1
group by RSV, ADM_MON, GENDER

-- COMMAND ----------

select RSV, ADM_MON, GENDER, count(distinct pat_key) as RSV_GENDER_VENT 
from IN_RESP_DUP_1
WHERE VENT=1 AND RSV=1
group by RSV, ADM_MON, GENDER

-- COMMAND ----------

select RSV, ADM_MON, GENDER, count(distinct pat_key) as RSV_GENDER_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1
group by RSV, ADM_MON, GENDER

-- COMMAND ----------

select FLU, ADM_MON, GENDER, count(distinct pat_key) as FLU_GENDER
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, GENDER

-- COMMAND ----------

select FLU, ADM_MON, GENDER, count(distinct pat_key) as FLU_GENDER_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, GENDER

-- COMMAND ----------

select FLU, ADM_MON, GENDER, count(distinct pat_key) as FLU_GENDER_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, GENDER

-- COMMAND ----------

select FLU, ADM_MON, GENDER, count(distinct pat_key) as FLU_GENDER_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1
group by FLU, ADM_MON, GENDER

-- COMMAND ----------

-- MAGIC %md # CONDITION BY AGE
-- MAGIC

-- COMMAND ----------

select COVID, ADM_MON, AGECAT, count(distinct pat_key) as COVID_AGECAT 
from IN_RESP_DUP_1
WHERE COVID=1
group by COVID, ADM_MON, AGECAT

-- COMMAND ----------

select COVID, ADM_MON, AGECAT, count(distinct pat_key) as COVID_AGECAT_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1
group by COVID, ADM_MON, AGECAT

-- COMMAND ----------

select COVID, ADM_MON, AGECAT, count(distinct pat_key) as COVID_AGECAT_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1
group by COVID, ADM_MON, AGECAT

-- COMMAND ----------

select COVID, ADM_MON, AGECAT, count(distinct pat_key) as COVID_AGECAT_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1
group by COVID, ADM_MON, AGECAT

-- COMMAND ----------

select RSV, ADM_MON, AGECAT, count(distinct pat_key) as RSV_AGECAT 
from IN_RESP_DUP_1
WHERE RSV=1
group by RSV, ADM_MON, AGECAT

-- COMMAND ----------

select RSV, ADM_MON, AGECAT, count(distinct pat_key) as RSV_AGECAT_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1
group by RSV, ADM_MON, AGECAT

-- COMMAND ----------

select RSV, ADM_MON, AGECAT, count(distinct pat_key) as RSV_AGECAT_VENT 
from IN_RESP_DUP_1
WHERE VENT=1 AND RSV=1
group by RSV, ADM_MON, AGECAT

-- COMMAND ----------

select RSV, ADM_MON, AGECAT, count(distinct pat_key) as RSV_AGECAT_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1
group by RSV, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

select FLU, ADM_MON, AGECAT, count(distinct pat_key) as FLU_AGECAT_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1
group by FLU, ADM_MON, AGECAT

-- COMMAND ----------

-- MAGIC %md # CONDITION BY RACE
-- MAGIC

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH 
from IN_RESP_DUP_1
WHERE COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH 
from IN_RESP_DUP_1
WHERE RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_VENT 
from IN_RESP_DUP_1
WHERE VENT=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH 
from IN_RESP_DUP_1
WHERE COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select COVID, ADM_MON, RACEETH, count(distinct pat_key) as COVID_RACEETH_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND COVID=1
group by COVID, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH 
from IN_RESP_DUP_1
WHERE RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_ICU 
from IN_RESP_DUP_1
WHERE ICU=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_VENT 
from IN_RESP_DUP_1 
WHERE VENT=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select RSV, ADM_MON, RACEETH, count(distinct pat_key) as RSV_RACEETH_EXPIRED 
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND RSV=1
group by RSV, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_EXPIRED
from IN_RESP_DUP_1
WHERE EXPIRED=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH
from IN_RESP_DUP_1
WHERE FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_ICU
from IN_RESP_DUP_1
WHERE ICU=1 AND FLU=1
group by FLU, ADM_MON, RACEETH

-- COMMAND ----------

select FLU, ADM_MON, RACEETH, count(distinct pat_key) as FLU_RACEETH_VENT
from IN_RESP_DUP_1
WHERE VENT=1 AND FLU=1
group by FLU, ADM_MON, RACEETH
