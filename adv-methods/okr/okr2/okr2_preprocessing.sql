-- Databricks notebook source
create or replace table cdh_mimic_exploratory.nlp_biobert_note_entities as 
select  note_id, snippet, nlp_category, count(score) as counts ---note_id,count(nlp_category) as nlp_categories
 from  note_ht_sentences_biobert_nlp
group by  note_id, snippet, nlp_category


-- COMMAND ----------


