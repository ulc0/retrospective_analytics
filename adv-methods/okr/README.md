# Accuracy OKR Calculation

--------------- 

## Notebook

The notebook `nb_parameterized_okr_code` is used to calculate the accuracy OKR metric. The purpose of the accuracy metric is to determine how good the NER is at identifying entities related to a particular condition, for a subset of patients that we know have been diagnosed with that condition. The accuracy metric is calculated for a given condition (e.g. hypertension), that must be specified in a configuration file

## Customization

The notebook is fully parameterized and be customized for different conditions/datasets. In order to modify the notebook for your specific use case, you need to modify two things: 

1. The config file: `okr_config.yml`
2. The notebook level widgets

The config file should be located in the same directory as the notebook, and the notebook level widgets are set via the notebook itself  
See the following sections for a detailed description of the exact values that must be set.

## Configuration

The configuration file is `okr_config.yml`, located in the same directory as the `nb_parameterzied_okr_code` notebook. The four condition related parameters specified in this file are:

1. name
2. diagnosis_label
3. diagnosis_codes
4. keywords

name: The name of the condition  
diagnosis_label: An abbreviation for the condition  
diagnosis_codes: Diagnosis codes corresponding to the condition  
keywords: Keywords corresponding to the condition

## Parameters

There are 9 parameters that are set via Databricks widgets in the `nb_parameterized_okr_code` notebook 

1. catalog
2. experiment_id
3. fact_table_name
4. fact_table_schema
5. ner_table_name
6. ner_table_schema
7. note_table_name
8. note_table_schema
9. test_schema

The **experiment_id** paramter is used for tracking metrics with MLFlow  
All other parameters relate to the datasets used to calculate the accuracy OKR metric