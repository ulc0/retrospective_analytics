-- Databricks notebook source
  INSERT INTO cdh_premier_omop_etl.stage_person
  (
    person_source_value,
    gender,
    year_of_birth,
    death_datetime,
    race,
    ethnicity,
    ethnicity_source_value,
    gender_source_value,
    race_source_value,
    load_id
  )
  select distinct
      person_source_value,
      gender, -- F = Female, M = Male, U = Unknown,
      year_of_birth, --  age in years
      null as death_datetime,
      case race
          when 'W' then 'White'
          when 'B' then 'Black'
          when 'A' then 'Asian'
          when 'O' then 'Other'
          else null
      end as race,
      case hispanic_ind
          when 'Y' then 'Hispanic'
          when 'N' then 'Not'
          else null
      end as ethnicity,
      hispanic_ind as ethnicity_source_value,
      gender as gender_source_value,
      race as race_source_value,
      1 as load_id
  from
  (
      select distinct
        person_source_value,
        first_value( case when gender is not null then gender end) over (partition by person_source_value order by admit_date desc  ) as gender,
        first_value( case when race is not null then race end) over (partition by person_source_value order by admit_date desc  ) as race,
        first_value( case when hispanic_ind is not null then hispanic_ind end) over (partition by person_source_value order by admit_date desc  ) as hispanic_ind,
        first_value( case when year_of_birth is not null then year_of_birth end) over (partition by person_source_value order by admit_date desc  )  as year_of_birth
      from 
      (
        select distinct
            el.medrec_key as person_source_value,
            el.gender as gender, -- F = Female, M = Male, U = Unknown,
            left(el.admit_date,4) - el.age as year_of_birth, 
            el.age,
            el.race,
            el.hispanic_ind,
            el.admit_date
      from cdh_premier_v2.patdemo el
      )
  )
  ;
