# Databricks notebook source
from pyspark.sql.functions import col
from pathlib import Path
import os
dbutils_defined = 'dbutils' in locals() or 'dbutils' in globals()
if not dbutils_defined:
    from databricks.connect import DatabricksSession
    from databricks.sdk.core import Config

    databricks_profile = "truveta_respiratory_prod"
    databricks_profile = databricks_profile.upper()

    user_name = os.environ.get("USER") or os.environ.get("USERNAME")
    os.environ["USER_ID"] = user_name
    config = Config(profile=databricks_profile)
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
                
if dbutils_defined:

    dbutils.widgets.text('target_database', 'edav_prd_cdh.cdh_truveta_lava_dev')


if dbutils_defined:

    dbutils.widgets.text('target_database', 'edav_prd_cdh.cdh_truveta_lava_dev')


if dbutils_defined:

    dbutils.widgets.text('source_database', 'edav_prd_cdh.cdh_truveta')


if dbutils_defined:

    dbutils.widgets.text('source_database', 'edav_prd_cdh.cdh_truveta')


if dbutils_defined:

    dbutils.widgets.text('yyyy', '2024')


if dbutils_defined:

    dbutils.widgets.text('yyyy', '2024')


if dbutils_defined:

    dbutils.widgets.text('mm', '04')


if dbutils_defined:

    dbutils.widgets.text('mm', '04')


if dbutils_defined:

    dbutils.widgets.text('dd', 'NA')


if dbutils_defined:

    dbutils.widgets.text('dd', 'NA')


if dbutils_defined:

    dbutils.widgets.text('transmission_period', '04_2024')


if dbutils_defined:

    dbutils.widgets.text('transmission_period', '04_2024')

dict_parameters = {'target_database':'edav_prd_cdh.cdh_truveta_lava_dev','source_database':'edav_prd_cdh.cdh_truveta','yyyy':'2024','mm':'04','dd':'NA','transmission_period':'04_2024','environment':'prod'}

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA
CLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  id encounterId,
  patientid,
  PersonId,
  classConceptId,
  startdatetime
FROM
  {source_database}.encounter
WHERE
  startdatetime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA
CLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  id encounterId,
  patientid,
  PersonId,
  classConceptId,
  startdatetime
FROM
  {source_database}.encounter
WHERE
  startdatetime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA

CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  encounterid,
  codeconceptmapid,
  recordeddatetime
FROM
  {source_database}.procedure
WHERE
  RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA

CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  encounterid,
  codeconceptmapid,
  recordeddatetime
FROM
  {source_database}.procedure
WHERE
  RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
 TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.condition o
    JOIN {source_database}.conditioncodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
 TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.condition o
    JOIN {source_database}.conditioncodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  conlabid.virus_type AS virus_type_code
FROM
  {source_database}.observation o
    JOIN {source_database}.observationcodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      Select
        virus_type,
        t_virus_type.conceptcode,
        conceptid
      from
        (
          Select
            'covid' as virus_type,
            diag_loinc_lab_covid_code as conceptcode
          from
            {target_database}.code_diag_loinc_lab_covid
          union
          Select
            'flu' as virus_type,
            diag_loinc_lab_flu_code conceptcode
          from
            {target_database}.code_diag_loinc_lab_flu
        ) as t_virus_type
          left join {source_database}.concept cn
            on cn.conceptcode = t_virus_type.conceptcode
      where
        (
          codesystem in ('LOINC')
          or codesystem is null
        )
    ) AS conlabid
      ON conlabid.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  conlabid.virus_type AS virus_type_code
FROM
  {source_database}.observation o
    JOIN {source_database}.observationcodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      Select
        virus_type,
        t_virus_type.conceptcode,
        conceptid
      from
        (
          Select
            'covid' as virus_type,
            diag_loinc_lab_covid_code as conceptcode
          from
            {target_database}.code_diag_loinc_lab_covid
          union
          Select
            'flu' as virus_type,
            diag_loinc_lab_flu_code conceptcode
          from
            {target_database}.code_diag_loinc_lab_flu
        ) as t_virus_type
          left join {source_database}.concept cn
            on cn.conceptcode = t_virus_type.conceptcode
      where
        (
          codesystem in ('LOINC')
          or codesystem is null
        )
    ) AS conlabid
      ON conlabid.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.observation o
    JOIN {source_database}.observationcodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS
SELECT
  o.encounterid,
  o.codeconceptmapid,
  o.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.observation o
    JOIN {source_database}.observationcodeconceptmap cm
      ON o.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  o.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  p.encounterid,
  p.codeconceptmapid,
  p.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.procedure p
    JOIN {source_database}.procedurecodeconceptmap cm
      ON p.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  p.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  p.encounterid,
  p.codeconceptmapid,
  p.recordeddatetime,
  condit.virus_type AS virus_type_code
FROM
  {source_database}.procedure p
    JOIN {source_database}.procedurecodeconceptmap cm
      ON p.codeconceptmapid = cm.id
    JOIN (
      -- COVID concepts
      SELECT
        'covid' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_covid codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_covid_code
      WHERE
        cn.codesystem = 'ICD10CM'
      UNION
      -- Flu concepts
      SELECT
        'flu' AS virus_type,
        cn.conceptid
      FROM
        {target_database}.code_diag_icd_flu codes
          JOIN {source_database}.concept cn
            ON cn.conceptcode = codes.diag_icd_flu_code
      WHERE
        cn.codesystem = 'ICD10CM'
    ) AS condit
      ON condit.conceptid = cm.codeconceptid
WHERE
  p.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA
CLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  c.encounterid,
  c.codeconceptmapid,
  c.effectivedatetime,
  c.NormalizedValueConceptId,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.labresult c
    JOIN {source_database}.labresultcodeconceptmap cm
      ON c.codeconceptmapid = cm.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = cm.codeconceptid
    JOIN {target_database}.code_inclusion_exclusion_labs labs
      ON (
        c.NormalizedValueConceptId IN (
          SELECT
            conceptid
          FROM
            {source_database}.concept
          WHERE
            conceptcode = labs.inclusion_exclusion_labs_code
            AND labs.inclusion_exclusion_labs_category = 'Positive'
        )
      )
WHERE
  c.EffectiveDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA
CLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  c.encounterid,
  c.codeconceptmapid,
  c.effectivedatetime,
  c.NormalizedValueConceptId,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.labresult c
    JOIN {source_database}.labresultcodeconceptmap cm
      ON c.codeconceptmapid = cm.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = cm.codeconceptid
    JOIN {target_database}.code_inclusion_exclusion_labs labs
      ON (
        c.NormalizedValueConceptId IN (
          SELECT
            conceptid
          FROM
            {source_database}.concept
          WHERE
            conceptcode = labs.inclusion_exclusion_labs_code
            AND labs.inclusion_exclusion_labs_category = 'Positive'
        )
      )
WHERE
  c.EffectiveDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA
CLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  mr.encounterid,
  mr.codeconceptmapid,
  mr.authoredondatetime,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationrequest mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  mr.AuthoredOnDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA
CLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  mr.encounterid,
  mr.codeconceptmapid,
  mr.authoredondatetime,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationrequest mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  mr.AuthoredOnDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA
CLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  'Unknown'  encounterid,
  mr.codeconceptmapid,
  mr.recordeddatetime  ,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationdispense mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  recordeddatetime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA
CLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  'Unknown'  encounterid,
  mr.codeconceptmapid,
  mr.recordeddatetime  ,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationdispense mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  recordeddatetime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  mr.encounterid,
  mr.codeconceptmapid,
  mr.recordeddatetime,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationadministration mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  mr.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA
CLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS
SELECT
  mr.encounterid,
  mr.codeconceptmapid,
  mr.recordeddatetime,
  conid.virus_type AS virus_type_code
FROM
  {source_database}.medicationadministration mr
    JOIN {source_database}.medicationcodeconceptmap mp
      ON mr.codeconceptmapid = mp.id
    JOIN (
      -- Combined COVID and Flu concepts (both ICD and LOINC)
      SELECT
        virus_type,
        conceptcode,
        conceptid
      FROM
        (
          -- Lab concepts (LOINC)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_loinc_lab_covid_code as conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_loinc_lab_flu_code conceptcode
              FROM
                {target_database}.code_diag_loinc_lab_flu
            ) as t_virus_type
              LEFT JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            (
              codesystem IN ('LOINC')
              OR codesystem IS NULL
            )
          UNION
          -- Condition concepts (ICD10CM)
          SELECT
            virus_type,
            cn.conceptcode,
            conceptid
          FROM
            (
              SELECT
                'covid' as virus_type,
                diag_icd_covid_code as conceptcode
              FROM
                {target_database}.code_diag_icd_covid
              UNION
              SELECT
                'flu' as virus_type,
                diag_icd_flu_code conceptcode
              FROM
                {target_database}.code_diag_icd_flu
            ) as t_virus_type
              JOIN {source_database}.concept cn
                ON cn.conceptcode = t_virus_type.conceptcode
          WHERE
            cn.codesystem = 'ICD10CM'
        )
    ) AS conid
      ON conid.conceptid = mp.codeconceptid
WHERE
  mr.RecordedDateTime >= '2022-08-01'""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA
CLUSTER BY (EncounterId) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS

  with 
   
  cond as (select
    encounterid,
     virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_condition c
 
  ),

  medreq as (
    Select  encounterid,  virus_type_code, authoredondatetime as dt from 
    {target_database}.optimized_medicationrequest   
  )
  
  ,
  medadm as (
    Select  encounterid, virus_type_code, recordeddatetime as dt from 
       {target_database}.optimized_medicationadministration
   
  ),
  meddisp as (
    Select 'Unknown' encounterid, virus_type_code, recordeddatetime as dt from 
    {target_database}.optimized_medicationadministration
  ),
  lab as
   (select
    encounterid,
    virus_type_code, effectivedatetime as dt
    from
       {target_database}.optimized_labresult 
    )
  ,
 obs as (select -- Observation based on condition
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_observation_condition c
    union
    select  -- Observation based on labs
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_observation_lab
  ),
   proc as (select
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
     {target_database}.optimized_procedure
  ),
  all_enc as (
                select encounterid, virus_type_code from cond union
                select encounterid, virus_type_code from medreq union --no inpact
                select encounterid, virus_type_code from medadm union  --no inpact
                select encounterid, virus_type_code from meddisp union  --no inpact
                select encounterid, virus_type_code from lab union
                select encounterid, virus_type_code from obs union
                select encounterid, virus_type_code from proc --no inpact
            )
  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA
CLUSTER BY (EncounterId) -- enables Liquid Clustering
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS

  with 
   
  cond as (select
    encounterid,
     virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_condition c
 
  ),

  medreq as (
    Select  encounterid,  virus_type_code, authoredondatetime as dt from 
    {target_database}.optimized_medicationrequest   
  )
  
  ,
  medadm as (
    Select  encounterid, virus_type_code, recordeddatetime as dt from 
       {target_database}.optimized_medicationadministration
   
  ),
  meddisp as (
    Select 'Unknown' encounterid, virus_type_code, recordeddatetime as dt from 
    {target_database}.optimized_medicationadministration
  ),
  lab as
   (select
    encounterid,
    virus_type_code, effectivedatetime as dt
    from
       {target_database}.optimized_labresult 
    )
  ,
 obs as (select -- Observation based on condition
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_observation_condition c
    union
    select  -- Observation based on labs
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
    {target_database}.optimized_observation_lab
  ),
   proc as (select
    encounterid,
    virus_type_code, recordeddatetime as dt
  from
     {target_database}.optimized_procedure
  ),
  all_enc as (
                select encounterid, virus_type_code from cond union
                select encounterid, virus_type_code from medreq union --no inpact
                select encounterid, virus_type_code from medadm union  --no inpact
                select encounterid, virus_type_code from meddisp union  --no inpact
                select encounterid, virus_type_code from lab union
                select encounterid, virus_type_code from obs union
                select encounterid, virus_type_code from proc --no inpact
            )
  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    
"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter 
USING DELTA
CLUSTER BY (EncounterId, patientid, startdatetime)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS

WITH base AS (
  SELECT  
    enc.encounterId,
    startdatetime,
    patientid,
    enc.personid,
    classconceptid,
    CASE  
      WHEN CAST(startdatetime AS DATE) BETWEEN DATE('2022-09-01') AND DATE('2023-08-31') THEN 'Sep 2022 - Aug 2023'
      WHEN CAST(startdatetime AS DATE) BETWEEN DATE('2023-09-01') AND DATE('2024-08-31') THEN 'Sep 2023 - Aug 2024'
      WHEN CAST(startdatetime AS DATE) >= DATE('2024-09-01') THEN 'Sep 2024 - Onwards'
      ELSE null
    END AS season,
    pn.ethnicityconceptid AS ethnicity_code,
    pr.RaceConceptId AS race_code,
    pn.GenderConceptId,
    pn.BirthDateTime
  FROM {target_database}.optimized_encounter enc
    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code
    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id
    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid
    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid
  WHERE startdatetime >= '2022-08-01'
    AND startdatetime <= now()
    AND  c_enc.health_settings_category is not null
)

SELECT *,
  DATEDIFF(
    MIN(startdatetime) OVER (PARTITION BY patientid, season),
    DATE_ADD(DATE_TRUNC('MM', BirthDateTime), 14)
  ) AS seasonal_index_age_days,

    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,

    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,

  -- Standard 8-bin code
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '<0.6'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '0.6-<2'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN '2-4'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN '5-17'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN '18-49'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN '50-64'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65-74'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+'
    ELSE null
  END AS range_agecat_8bin_code,

  -- Human-readable 8-bin
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '0 < 6 mon'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '6 mon- < 2 yrs'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN '2 - 4 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN '5 - 17 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN '18 - 49 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN '50 - 64 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65 - 74 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+ yrs'
    ELSE null
  END AS range_agecat_8bin,

  -- Pregnant age codes
  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN '15-17'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN '18-24'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN '25-39'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN '40-54'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN '55+'
    ELSE null
  END AS range_agecat_pregnant_code,

  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN '15 - 17 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN '18 - 24 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN '25 - 39 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN '40 - 54 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN '55 + yrs'
    ELSE null
  END AS range_agecat_pregnant,

  -- Pediatric codes
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '<0.6'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '0.6-<2'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    ELSE null
  END AS range_agecat_pediatric_code,

  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '0 < 6 mon'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '6 mon- < 2 yrs'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '6 mon- < 2 yrs'
    ELSE null
  END AS range_agecat_pediatric,

  -- Older adult codes
  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65-74'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+'
    ELSE null
  END AS range_agecat_older_adult_code,

  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65 - 74 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+ yrs'
    ELSE null
  END AS range_agecat_older_adult

FROM base""".format(**dict_parameters)

# COMMAND ----------

# print(['"""CREATE OR REPLACE TABLE {target_database}.optimized_encounter USING DELTA\nCLUSTER BY (EncounterId, startdatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  id encounterId,\n  patientid,\n  PersonId,\n  classConceptId,\n  startdatetime\nFROM\n  {source_database}.encounter\nWHERE\n  startdatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\n\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  encounterid,\n  codeconceptmapid,\n  recordeddatetime\nFROM\n  {source_database}.procedure\nWHERE\n  RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_condition USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\n TBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.condition o\n    JOIN {source_database}.conditioncodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_lab AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  conlabid.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      Select\n        virus_type,\n        t_virus_type.conceptcode,\n        conceptid\n      from\n        (\n          Select\n            \'covid\' as virus_type,\n            diag_loinc_lab_covid_code as conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_covid\n          union\n          Select\n            \'flu\' as virus_type,\n            diag_loinc_lab_flu_code conceptcode\n          from\n            {target_database}.code_diag_loinc_lab_flu\n        ) as t_virus_type\n          left join {source_database}.concept cn\n            on cn.conceptcode = t_virus_type.conceptcode\n      where\n        (\n          codesystem in (\'LOINC\')\n          or codesystem is null\n        )\n    ) AS conlabid\n      ON conlabid.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_observation_condition AS\nSELECT\n  o.encounterid,\n  o.codeconceptmapid,\n  o.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.observation o\n    JOIN {source_database}.observationcodeconceptmap cm\n      ON o.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  o.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_procedure USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  p.encounterid,\n  p.codeconceptmapid,\n  p.recordeddatetime,\n  condit.virus_type AS virus_type_code\nFROM\n  {source_database}.procedure p\n    JOIN {source_database}.procedurecodeconceptmap cm\n      ON p.codeconceptmapid = cm.id\n    JOIN (\n      -- COVID concepts\n      SELECT\n        \'covid\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_covid codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_covid_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n      UNION\n      -- Flu concepts\n      SELECT\n        \'flu\' AS virus_type,\n        cn.conceptid\n      FROM\n        {target_database}.code_diag_icd_flu codes\n          JOIN {source_database}.concept cn\n            ON cn.conceptcode = codes.diag_icd_flu_code\n      WHERE\n        cn.codesystem = \'ICD10CM\'\n    ) AS condit\n      ON condit.conceptid = cm.codeconceptid\nWHERE\n  p.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_labresult USING DELTA\nCLUSTER BY (EncounterId, EffectiveDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  c.encounterid,\n  c.codeconceptmapid,\n  c.effectivedatetime,\n  c.NormalizedValueConceptId,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.labresult c\n    JOIN {source_database}.labresultcodeconceptmap cm\n      ON c.codeconceptmapid = cm.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = cm.codeconceptid\n    JOIN {target_database}.code_inclusion_exclusion_labs labs\n      ON (\n        c.NormalizedValueConceptId IN (\n          SELECT\n            conceptid\n          FROM\n            {source_database}.concept\n          WHERE\n            conceptcode = labs.inclusion_exclusion_labs_code\n            AND labs.inclusion_exclusion_labs_category = \'Positive\'\n        )\n      )\nWHERE\n  c.EffectiveDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationrequest USING DELTA\nCLUSTER BY (EncounterId, AuthoredOnDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.authoredondatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationrequest mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.AuthoredOnDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_medicationdispense USING DELTA\nCLUSTER BY (EncounterId, recordeddatetime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  \'Unknown\'  encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime  ,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationdispense mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  recordeddatetime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE  TABLE {target_database}.optimized_medicationadministration USING DELTA\nCLUSTER BY (EncounterId, RecordedDateTime) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\nSELECT\n  mr.encounterid,\n  mr.codeconceptmapid,\n  mr.recordeddatetime,\n  conid.virus_type AS virus_type_code\nFROM\n  {source_database}.medicationadministration mr\n    JOIN {source_database}.medicationcodeconceptmap mp\n      ON mr.codeconceptmapid = mp.id\n    JOIN (\n      -- Combined COVID and Flu concepts (both ICD and LOINC)\n      SELECT\n        virus_type,\n        conceptcode,\n        conceptid\n      FROM\n        (\n          -- Lab concepts (LOINC)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_loinc_lab_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_loinc_lab_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_loinc_lab_flu\n            ) as t_virus_type\n              LEFT JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            (\n              codesystem IN (\'LOINC\')\n              OR codesystem IS NULL\n            )\n          UNION\n          -- Condition concepts (ICD10CM)\n          SELECT\n            virus_type,\n            cn.conceptcode,\n            conceptid\n          FROM\n            (\n              SELECT\n                \'covid\' as virus_type,\n                diag_icd_covid_code as conceptcode\n              FROM\n                {target_database}.code_diag_icd_covid\n              UNION\n              SELECT\n                \'flu\' as virus_type,\n                diag_icd_flu_code conceptcode\n              FROM\n                {target_database}.code_diag_icd_flu\n            ) as t_virus_type\n              JOIN {source_database}.concept cn\n                ON cn.conceptcode = t_virus_type.conceptcode\n          WHERE\n            cn.codesystem = \'ICD10CM\'\n        )\n    ) AS conid\n      ON conid.conceptid = mp.codeconceptid\nWHERE\n  mr.RecordedDateTime >= \'2022-08-01\'""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_allenc USING DELTA\nCLUSTER BY (EncounterId) -- enables Liquid Clustering\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\n  with \n   \n  cond as (select\n    encounterid,\n     virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_condition c\n \n  ),\n\n  medreq as (\n    Select  encounterid,  virus_type_code, authoredondatetime as dt from \n    {target_database}.optimized_medicationrequest   \n  )\n  \n  ,\n  medadm as (\n    Select  encounterid, virus_type_code, recordeddatetime as dt from \n       {target_database}.optimized_medicationadministration\n   \n  ),\n  meddisp as (\n    Select \'Unknown\' encounterid, virus_type_code, recordeddatetime as dt from \n    {target_database}.optimized_medicationadministration\n  ),\n  lab as\n   (select\n    encounterid,\n    virus_type_code, effectivedatetime as dt\n    from\n       {target_database}.optimized_labresult \n    )\n  ,\n obs as (select -- Observation based on condition\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_condition c\n    union\n    select  -- Observation based on labs\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n    {target_database}.optimized_observation_lab\n  ),\n   proc as (select\n    encounterid,\n    virus_type_code, recordeddatetime as dt\n  from\n     {target_database}.optimized_procedure\n  ),\n  all_enc as (\n                select encounterid, virus_type_code from cond union\n                select encounterid, virus_type_code from medreq union --no inpact\n                select encounterid, virus_type_code from medadm union  --no inpact\n                select encounterid, virus_type_code from meddisp union  --no inpact\n                select encounterid, virus_type_code from lab union\n                select encounterid, virus_type_code from obs union\n                select encounterid, virus_type_code from proc --no inpact\n            )\n  Select  encounterid, virus_type_code from all_enc""".format(**dict_parameters)', '"""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter \nUSING DELTA\nCLUSTER BY (EncounterId, patientid, startdatetime)\nTBLPROPERTIES (\n  \'delta.autoOptimize.optimizeWrite\' = \'true\',\n  \'delta.autoOptimize.autoCompact\' = \'true\',\n  \'delta.deletedFileRetentionDuration\' = \'interval 7 days\',\n  \'delta.enableChangeDataCapture\' = \'false\'\n) AS\n\nWITH base AS (\n  SELECT  \n    enc.encounterId,\n    startdatetime,\n    patientid,\n    enc.personid,\n    classconceptid,\n    CASE  \n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2022-09-01\') AND DATE(\'2023-08-31\') THEN \'Sep 2022 - Aug 2023\'\n      WHEN CAST(startdatetime AS DATE) BETWEEN DATE(\'2023-09-01\') AND DATE(\'2024-08-31\') THEN \'Sep 2023 - Aug 2024\'\n      WHEN CAST(startdatetime AS DATE) >= DATE(\'2024-09-01\') THEN \'Sep 2024 - Onwards\'\n      ELSE null\n    END AS season,\n    pn.ethnicityconceptid AS ethnicity_code,\n    pr.RaceConceptId AS race_code,\n    pn.GenderConceptId,\n    pn.BirthDateTime\n  FROM {target_database}.optimized_encounter enc\n    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code\n    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id\n    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid\n    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid\n  WHERE startdatetime >= \'2022-08-01\'\n    AND startdatetime <= now()\n    AND  c_enc.health_settings_category is not null\n)\n\nSELECT *,\n  DATEDIFF(\n    MIN(startdatetime) OVER (PARTITION BY patientid, season),\n    DATE_ADD(DATE_TRUNC(\'MM\', BirthDateTime), 14)\n  ) AS seasonal_index_age_days,\n\n    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,\n\n    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,\n\n  -- Standard 8-bin code\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2-4\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18-49\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50-64\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_8bin_code,\n\n  -- Human-readable 8-bin\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN \'2 - 4 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN \'5 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN \'18 - 49 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN \'50 - 64 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_8bin,\n\n  -- Pregnant age codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15-17\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18-24\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25-39\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40-54\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55+\'\n    ELSE null\n  END AS range_agecat_pregnant_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN \'15 - 17 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN \'18 - 24 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN \'25 - 39 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN \'40 - 54 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN \'55 + yrs\'\n    ELSE null\n  END AS range_agecat_pregnant,\n\n  -- Pediatric codes\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'<0.6\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'0.6-<2\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'0.6-<2\'\n    ELSE null\n  END AS range_agecat_pediatric_code,\n\n  CASE\n    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN \'0 < 6 mon\'\n    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN \'6 mon- < 2 yrs\'\n    WHEN (seasonal_index_age_days / 365.25) LIKE \'1.99%\' THEN \'6 mon- < 2 yrs\'\n    ELSE null\n  END AS range_agecat_pediatric,\n\n  -- Older adult codes\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65-74\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+\'\n    ELSE null\n  END AS range_agecat_older_adult_code,\n\n  CASE\n    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN \'65 - 74 yrs\'\n    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN \'75+ yrs\'\n    ELSE null\n  END AS range_agecat_older_adult\n\nFROM base""".format(**dict_parameters)'])

# COMMAND ----------

execute_results_flag = "skip_execute"

# COMMAND ----------

df_results = spark.sql("""CREATE OR REPLACE TABLE {target_database}.optimized_base_encounter 
USING DELTA
CLUSTER BY (EncounterId, patientid, startdatetime)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataCapture' = 'false'
) AS

WITH base AS (
  SELECT  
    enc.encounterId,
    startdatetime,
    patientid,
    enc.personid,
    classconceptid,
    CASE  
      WHEN CAST(startdatetime AS DATE) BETWEEN DATE('2022-09-01') AND DATE('2023-08-31') THEN 'Sep 2022 - Aug 2023'
      WHEN CAST(startdatetime AS DATE) BETWEEN DATE('2023-09-01') AND DATE('2024-08-31') THEN 'Sep 2023 - Aug 2024'
      WHEN CAST(startdatetime AS DATE) >= DATE('2024-09-01') THEN 'Sep 2024 - Onwards'
      ELSE null
    END AS season,
    pn.ethnicityconceptid AS ethnicity_code,
    pr.RaceConceptId AS race_code,
    pn.GenderConceptId,
    pn.BirthDateTime
  FROM {target_database}.optimized_encounter enc
    JOIN {target_database}.code_health_settings c_enc on enc.ClassConceptId = c_enc.health_settings_code
    LEFT JOIN {source_database}.patient pat ON enc.patientid = pat.id
    LEFT JOIN {source_database}.person pn ON pn.id = enc.personid
    LEFT JOIN {source_database}.personrace pr ON pr.personid = enc.personid
  WHERE startdatetime >= '2022-08-01'
    AND startdatetime <= now()
    AND  c_enc.health_settings_category is not null
)

SELECT *,
  DATEDIFF(
    MIN(startdatetime) OVER (PARTITION BY patientid, season),
    DATE_ADD(DATE_TRUNC('MM', BirthDateTime), 14)
  ) AS seasonal_index_age_days,

    min(startdatetime) over(PARTITION BY patientid, season) as indexdate,

    case when datediff(startdatetime,min(startdatetime) over(PARTITION BY patientid, season)) between -1 and 16 then 1 else 0 end as win_1to16flag,

  -- Standard 8-bin code
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '<0.6'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '0.6-<2'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN '2-4'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN '5-17'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN '18-49'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN '50-64'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65-74'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+'
    ELSE null
  END AS range_agecat_8bin_code,

  -- Human-readable 8-bin
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '0 < 6 mon'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '6 mon- < 2 yrs'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 2 AND 4 THEN '2 - 4 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 5 AND 17 THEN '5 - 17 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 49 THEN '18 - 49 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 50 AND 64 THEN '50 - 64 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65 - 74 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+ yrs'
    ELSE null
  END AS range_agecat_8bin,

  -- Pregnant age codes
  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN '15-17'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN '18-24'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN '25-39'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN '40-54'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN '55+'
    ELSE null
  END AS range_agecat_pregnant_code,

  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 15 AND 17 THEN '15 - 17 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 18 AND 24 THEN '18 - 24 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 25 AND 39 THEN '25 - 39 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 40 AND 54 THEN '40 - 54 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 55 THEN '55 + yrs'
    ELSE null
  END AS range_agecat_pregnant,

  -- Pediatric codes
  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '<0.6'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '0.6-<2'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '0.6-<2'
    ELSE null
  END AS range_agecat_pediatric_code,

  CASE
    WHEN seasonal_index_age_days BETWEEN 0 AND 179 THEN '0 < 6 mon'
    WHEN seasonal_index_age_days BETWEEN 180 AND 729 THEN '6 mon- < 2 yrs'
    WHEN (seasonal_index_age_days / 365.25) LIKE '1.99%' THEN '6 mon- < 2 yrs'
    ELSE null
  END AS range_agecat_pediatric,

  -- Older adult codes
  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65-74'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+'
    ELSE null
  END AS range_agecat_older_adult_code,

  CASE
    WHEN FLOOR(seasonal_index_age_days / 365.25) BETWEEN 65 AND 74 THEN '65 - 74 yrs'
    WHEN FLOOR(seasonal_index_age_days / 365.25) >= 75 THEN '75+ yrs'
    ELSE null
  END AS range_agecat_older_adult

FROM base""".format(**dict_parameters))
df_results.show()
listColumns=df_results.columns
#if ("sql_statement"  in listColumns):
#    print(df_results.first().sql_statement)
if (df_results.count() > 0):
    if ("sql_statement" in listColumns):
        df_merge = spark.sql(df_results.first().sql_statement)
        df_merge.show()

# COMMAND ----------

    