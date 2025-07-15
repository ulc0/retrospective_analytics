# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_omop_etl")
dbutils.widgets.text("omop_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop")
dbutils.widgets.text("results_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("results_schema",defaultValue="cdh_premier_atlas_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC 		drop table if exists ${omop_catalog}.${omop_schema}.DRUG_ERA;
# MAGIC     CREATE TABLE ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC 	  (
# MAGIC 			drug_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id integer NOT NULL,
# MAGIC 			drug_concept_id integer NOT NULL,
# MAGIC 			drug_era_start_date timestamp NOT NULL,
# MAGIC 			drug_era_end_date timestamp NOT NULL,
# MAGIC 			drug_exposure_count integer,
# MAGIC 			gap_days integer )
# MAGIC 			   using delta		cluster by (person_id)
# MAGIC 			 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.DOSE_ERA;
# MAGIC
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.DOSE_ERA 
# MAGIC 	 (
# MAGIC 			dose_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id integer NOT NULL,
# MAGIC 			drug_concept_id integer NOT NULL,
# MAGIC 			unit_concept_id integer NOT NULL,
# MAGIC 			dose_value float NOT NULL,
# MAGIC 			dose_era_start_date timestamp NOT NULL,
# MAGIC 			dose_era_end_date timestamp NOT NULL )
# MAGIC 				 using delta cluster by (person_id)
# MAGIC 				  TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.CONDITION_ERA;
# MAGIC
# MAGIC CREATE TABLE ${omop_catalog}.${omop_schema}.CONDITION_ERA 
# MAGIC 	 (
# MAGIC 			condition_era_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id integer NOT NULL,
# MAGIC 			condition_concept_id integer NOT NULL,
# MAGIC 			condition_era_start_date timestamp NOT NULL,
# MAGIC 			condition_era_end_date timestamp NOT NULL,
# MAGIC 			condition_occurrence_count integer )
# MAGIC 				using delta  cluster by (person_id)
# MAGIC 				 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC     
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC    
# MAGIC     WITH cteConditionTarget (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date) AS
# MAGIC 		(
# MAGIC 			SELECT
# MAGIC 				co.condition_occurrence_id
# MAGIC 				, co.person_id
# MAGIC 				, co.condition_concept_id
# MAGIC 				, co.condition_start_date
# MAGIC 				, COALESCE(NULLIF(co.condition_end_date,NULL), condition_start_date + INTERVAL '1 day') AS condition_end_date
# MAGIC 			FROM ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC 			WHERE condition_concept_id != 0
# MAGIC 		),
# MAGIC 		--------------------------------------------------------------------------------------------------------------
# MAGIC 		cteEndDates (person_id, condition_concept_id, end_date) AS 
# MAGIC 		(
# MAGIC 			SELECT
# MAGIC 				person_id
# MAGIC 				, condition_concept_id
# MAGIC 				, event_date - INTERVAL '30 days' AS end_date 
# MAGIC 			FROM
# MAGIC 			(
# MAGIC 				SELECT
# MAGIC 					person_id
# MAGIC 					, condition_concept_id
# MAGIC 					, event_date
# MAGIC 					, event_type
# MAGIC 					, MAX(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
# MAGIC 					, ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord 
# MAGIC 				FROM
# MAGIC 				(
# MAGIC 					SELECT
# MAGIC 						person_id
# MAGIC 						, condition_concept_id
# MAGIC 						, condition_start_date AS event_date
# MAGIC 						, -1 AS event_type
# MAGIC 						, ROW_NUMBER() OVER (PARTITION BY person_id
# MAGIC 						, condition_concept_id ORDER BY condition_start_date) AS start_ordinal
# MAGIC 					FROM cteConditionTarget
# MAGIC 				
# MAGIC 					UNION ALL
# MAGIC 				
# MAGIC 					SELECT
# MAGIC 						person_id
# MAGIC 					       	, condition_concept_id
# MAGIC 						, condition_end_date + INTERVAL '30 days'
# MAGIC 						, 1 AS event_type
# MAGIC 						, NULL
# MAGIC 					FROM cteConditionTarget
# MAGIC 				) RAWDATA
# MAGIC 			) e
# MAGIC 			WHERE (2 * e.start_ordinal) - e.overall_ord = 0
# MAGIC 		),
# MAGIC 		--------------------------------------------------------------------------------------------------------------
# MAGIC 		cteConditionEnds (person_id, condition_concept_id, condition_start_date, era_end_date) AS
# MAGIC 		(
# MAGIC 		SELECT
# MAGIC 		        c.person_id
# MAGIC 			, c.condition_concept_id
# MAGIC 			, c.condition_start_date
# MAGIC 			, MIN(e.end_date) AS era_end_date
# MAGIC 		FROM cteConditionTarget c
# MAGIC 		JOIN cteEndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
# MAGIC 		GROUP BY
# MAGIC 		        c.condition_occurrence_id
# MAGIC 			, c.person_id
# MAGIC 			, c.condition_concept_id
# MAGIC 			, c.condition_start_date
# MAGIC 		)
# MAGIC 		--------------------------------------------------------------------------------------------------------------
# MAGIC 		INSERT INTO ${omop_catalog}.${omop_schema}.condition_era(person_id, condition_concept_id, condition_era_start_date, condition_era_end_date, condition_occurrence_count)
# MAGIC 		SELECT
# MAGIC 			person_id
# MAGIC 			, condition_concept_id
# MAGIC 			, MIN(condition_start_date) AS condition_era_start_date
# MAGIC 			, era_end_date AS condition_era_end_date
# MAGIC 			, COUNT(*) AS condition_occurrence_count
# MAGIC 		FROM cteConditionEnds
# MAGIC 		GROUP BY person_id, condition_concept_id, era_end_date
# MAGIC 		ORDER BY person_id, condition_concept_id
# MAGIC 		;
# MAGIC 			

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC WITH
# MAGIC ctePreDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS
# MAGIC (
# MAGIC 	SELECT
# MAGIC 		d.drug_exposure_id
# MAGIC 		, d.person_id
# MAGIC 		, c.concept_id AS ingredient_concept_id
# MAGIC 		, d.drug_exposure_start_date AS drug_exposure_start_date
# MAGIC 		, d.days_supply AS days_supply
# MAGIC 		, COALESCE(
# MAGIC 			NULLIF(drug_exposure_end_date, NULL) 
# MAGIC 			, NULLIF(drug_exposure_start_date + (INTERVAL '1 day' * days_supply), drug_exposure_start_date)
# MAGIC 			, drug_exposure_start_date + INTERVAL '1 day' 
# MAGIC 		) AS drug_exposure_end_date
# MAGIC 	FROM ${omop_catalog}.${omop_schema}.drug_exposure d
# MAGIC 	JOIN ${omop_catalog}.${omop_schema}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
# MAGIC 	JOIN ${omop_catalog}.${omop_schema}.concept c ON ca.ancestor_concept_id = c.concept_id
# MAGIC 	WHERE c.vocabulary_id = 'RxNorm' 
# MAGIC 	AND c.concept_class_id= 'Ingredient'
# MAGIC 	---AND d.drug_concept_id != 0
# MAGIC 	---AND d.days_supply >= 0
# MAGIC )
# MAGIC
# MAGIC , cteSubExposureEndDates (person_id, ingredient_concept_id, end_date) AS 
# MAGIC (
# MAGIC 	SELECT
# MAGIC 		person_id
# MAGIC 		, ingredient_concept_id
# MAGIC 		, event_date AS end_date
# MAGIC 	FROM
# MAGIC 	(
# MAGIC 		SELECT
# MAGIC 			person_id
# MAGIC 			, ingredient_concept_id
# MAGIC 			, event_date
# MAGIC 			, event_type
# MAGIC 			, MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal 
# MAGIC 			, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord 
# MAGIC 		FROM (
# MAGIC 	
# MAGIC 			SELECT
# MAGIC 				person_id
# MAGIC 				, ingredient_concept_id
# MAGIC 				, drug_exposure_start_date AS event_date
# MAGIC 				, -1 AS event_type
# MAGIC 				, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
# MAGIC 			FROM ctePreDrugTarget
# MAGIC 		
# MAGIC 			UNION ALL
# MAGIC
# MAGIC 			SELECT
# MAGIC 				person_id
# MAGIC 				, ingredient_concept_id
# MAGIC 				, drug_exposure_end_date
# MAGIC 				, 1 AS event_type
# MAGIC 				, NULL
# MAGIC 			FROM ctePreDrugTarget
# MAGIC 		) RAWDATA
# MAGIC 	) e
# MAGIC 	WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
# MAGIC )
# MAGIC 	
# MAGIC , cteDrugExposureEnds (person_id, drug_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date) AS
# MAGIC (
# MAGIC 	SELECT 
# MAGIC 	       dt.person_id
# MAGIC 	       , dt.ingredient_concept_id
# MAGIC 	       , dt.drug_exposure_start_date
# MAGIC 	       , MIN(e.end_date) AS drug_sub_exposure_end_date
# MAGIC 	FROM ctePreDrugTarget dt
# MAGIC 	JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
# MAGIC 	GROUP BY 
# MAGIC       	      dt.drug_exposure_id
# MAGIC       	      , dt.person_id
# MAGIC 	      , dt.ingredient_concept_id
# MAGIC 	      , dt.drug_exposure_start_date
# MAGIC )
# MAGIC --------------------------------------------------------------------------------------------------------------
# MAGIC , cteSubExposures(row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count) AS
# MAGIC (
# MAGIC 	SELECT
# MAGIC 		ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_date order by person_id, drug_concept_id)
# MAGIC 		, person_id
# MAGIC 		, drug_concept_id
# MAGIC 		, MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date
# MAGIC 		, drug_sub_exposure_end_date
# MAGIC 		, COUNT(*) AS drug_exposure_count
# MAGIC 	FROM cteDrugExposureEnds
# MAGIC 	GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_date
# MAGIC 	ORDER BY person_id, drug_concept_id
# MAGIC )
# MAGIC --------------------------------------------------------------------------------------------------------------
# MAGIC /*Everything above grouped exposures into sub_exposures if there was overlap between exposures.
# MAGIC  *This means no persistence window was implemented. Now we CAN add the persistence window to calculate eras.
# MAGIC  */
# MAGIC --------------------------------------------------------------------------------------------------------------
# MAGIC , cteFinalTarget(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count, days_exposed) AS
# MAGIC (
# MAGIC 	SELECT
# MAGIC 		row_number
# MAGIC 		, person_id
# MAGIC 		, drug_concept_id
# MAGIC 		, drug_sub_exposure_start_date
# MAGIC 		, drug_sub_exposure_end_date
# MAGIC 		, drug_exposure_count
# MAGIC 		, drug_sub_exposure_end_date - drug_sub_exposure_start_date AS days_exposed
# MAGIC 	FROM cteSubExposures
# MAGIC )
# MAGIC --------------------------------------------------------------------------------------------------------------
# MAGIC , cteEndDates (person_id, ingredient_concept_id, end_date) AS -- the magic
# MAGIC (
# MAGIC 	SELECT
# MAGIC 		person_id
# MAGIC 		, ingredient_concept_id
# MAGIC 		, event_date - INTERVAL '30 days' AS end_date -- unpad the end date
# MAGIC 	FROM
# MAGIC 	(
# MAGIC 		SELECT
# MAGIC 			person_id
# MAGIC 			, ingredient_concept_id
# MAGIC 			, event_date
# MAGIC 			, event_type
# MAGIC 			, MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
# MAGIC 			, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
# MAGIC 		FROM (
# MAGIC 			-- select the start dates, assigning a row number to each
# MAGIC 			SELECT
# MAGIC 				person_id
# MAGIC 				, ingredient_concept_id
# MAGIC 				, drug_sub_exposure_start_date AS event_date
# MAGIC 				, -1 AS event_type
# MAGIC 				, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
# MAGIC 			FROM cteFinalTarget
# MAGIC 		
# MAGIC 			UNION ALL
# MAGIC 		
# MAGIC 			-- pad the end dates by 30 to allow a grace period for overlapping ranges.
# MAGIC 			SELECT
# MAGIC 				person_id
# MAGIC 				, ingredient_concept_id
# MAGIC 				, drug_sub_exposure_end_date + INTERVAL '30 days'
# MAGIC 				, 1 AS event_type
# MAGIC 				, NULL
# MAGIC 			FROM cteFinalTarget
# MAGIC 		) RAWDATA
# MAGIC 	) e
# MAGIC 	WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
# MAGIC
# MAGIC )
# MAGIC , cteDrugEraEnds (person_id, drug_concept_id, drug_sub_exposure_start_date, drug_era_end_date, drug_exposure_count, days_exposed) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC 	ft.person_id
# MAGIC 	, ft.ingredient_concept_id
# MAGIC 	, ft.drug_sub_exposure_start_date
# MAGIC 	, MIN(e.end_date) AS era_end_date
# MAGIC 	, ft.drug_exposure_count
# MAGIC 	, ft.days_exposed
# MAGIC FROM cteFinalTarget ft
# MAGIC JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
# MAGIC GROUP BY 
# MAGIC       	ft.person_id
# MAGIC 	, ft.ingredient_concept_id
# MAGIC 	, ft.drug_sub_exposure_start_date
# MAGIC 	, ft.drug_exposure_count
# MAGIC 	, ft.days_exposed
# MAGIC )
# MAGIC INSERT INTO ${omop_catalog}.${omop_schema}.drug_era(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
# MAGIC SELECT
# MAGIC 	person_id
# MAGIC 	, drug_concept_id
# MAGIC 	, MIN(drug_sub_exposure_start_date) AS drug_era_start_date
# MAGIC 	, drug_era_end_date
# MAGIC 	, SUM(drug_exposure_count) AS drug_exposure_count
# MAGIC   , cast((drug_era_end_date - MIN(drug_sub_exposure_start_date) - SUM(days_exposed))/86400 as int) AS gap_days
# MAGIC 	-- , EXTRACT(EPOCH FROM drug_era_end_date - MIN(drug_sub_exposure_start_date) - SUM(days_exposed))/86400 AS gap_days
# MAGIC FROM cteDrugEraEnds
# MAGIC GROUP BY person_id, drug_concept_id, drug_era_end_date
# MAGIC ORDER BY person_id, drug_concept_id;
# MAGIC
