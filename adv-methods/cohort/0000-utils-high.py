# Databricks notebook source
"""
This notebook contains high-level functions for loading cleaned ABFM datasets.
Contact sve5@cdc.gov for questions.
""";

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------

def load_patient_table():
    """Returns a cleaned cdh_abfm_phi.patient table."""
        
    #define custom filters and windows
    isdeceased = (
    F.when(F.col('isdeceased')==True, F.col('modifieddate'))
    )

    patient_win = W.Window.partitionBy('patientuid')
    patient_win_date = patient_win.orderBy('modifieddate')
    
    #load data
    df = spark.table("cdh_abfm_phi.patient")
    
    #transform data
    df = (
        df
        .drop('countrycode','country','maritalstatustext','serviceprovidernpi','religiousaffiliationtext','state')
        .transform(clean_date('dob'))
        .transform(clean_date('modifieddate'))
        .transform(clean_date('createddate'))
        .transform(clean_gender())
        .transform(clean_zipcode())
        .transform(clean_state('statecode'))
        .transform(create_age('dob','modifieddate'))
        .withColumn('city', F.lower('city'))
        .withColumn('isdeceased', isdeceased)
        .withColumn('entries', F.row_number().over(patient_win_date))
        .withColumn('max_entries', F.count('patientuid').over(patient_win))
        .filter(F.col('entries')==F.col('max_entries'))
        .drop('max_entries')
    )
    
    return df

# COMMAND ----------

def load_patientproblem_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.patientproblem table."""
    
    df = spark.table("cdh_abfm_phi.patientproblem")
    
    cols = [
        'patientuid',
        'problemcode',
        'problemtext',
        'problemcategory',
        'documentationdate',
        'problemonsetdate',
        'problemresolutiondate',
        'practiceid'
    ] if subset else ['*']

    df = (
        df
        .select(*cols)
        .withColumn('problemtext',F.lower('problemtext'))
        .transform(clean_category_code())
        .transform(clean_date('documentationdate'))
        .transform(clean_date('problemonsetdate'))
        .transform(clean_date('problemresolutiondate'))
        .withColumn('problemcode', F.upper(_clean_input(F.col('problemcode'))))
         )
    
    return df

# COMMAND ----------

def load_visit_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.visit table."""
    
    df = spark.table("cdh_abfm_phi.visit")
    
    cols = [
        'patientuid',
        'encountertypecode',
        'encountertypetext',
        'encounterstartdate',
        'encounterenddate',
        'serviceprovidernpi',
        'service_location_state',
        'service_location_postalcode',
        'practiceid'
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
        .withColumn('encountertypecode',F.upper(_clean_input(F.col('encountertypecode'))))
        .transform(clean_date('encounterstartdate'))
        .transform(clean_date('encounterenddate'))
        .transform(clean_state('service_location_state'))
        .transform(clean_zipcode('service_location_postalcode'))
        .transform(assign_regions('service_location_state'))
        .filter(F.col('encounterstartdate').cast('date')<=F.to_date(F.current_date()))
        .withColumn('encountertypetext',F.lower(F.col('encountertypetext')))
    )
    
    return df

# COMMAND ----------

def load_visitdiagnosis_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.visitdiagnosis table."""
    
    df = spark.table("cdh_abfm_phi.visitdiagnosis")
    
    cols = [
        'patientuid',
        'encounterdate',
        'serviceprovidernpi',
        'encounterdiagnosiscode',
        'encounterdiagnosistext',
        'documentationdate',
        'practiceid'
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
        .transform(clean_date('encounterdate'))
        .filter(F.col('encounterdate').cast('date') <= F.to_date(F.current_date())) 
        .withColumn('encounterdiagnosiscode',F.upper(_clean_input(F.col('encounterdiagnosiscode'))))
        .withColumn('encounterdiagnosistext', F.lower(F.col('encounterdiagnosistext')))
        .transform(clean_date('documentationdate'))
    )
    
    return df

# COMMAND ----------

def load_raceeth_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.generatedraceethnicity table."""
    
    df = spark.table("cdh_abfm_phi.generatedraceethnicity")
    
    cols = [
        'patientuid',
        'Hispanic',
        'Race'
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
    )
    
    return df

# COMMAND ----------

def load_patientimmunization_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.patientimmunization table."""
    
    df = spark.table("cdh_abfm_phi.patientimmunization")
    
    cols = [
        'patientuid',
        'immunizationname',
        'immunizationstartdate',
        'immunizationstopdate',
        'administeredeffectivedate',
        'procedurecode',
        F.lower(F.col('proceduretext')).alias('proceduretext'),
        F.lower(F.col('medicationbrandname')).alias('medicationbrandname'),
        F.lower(F.col('medicationgenericname')).alias('medicationgenericname'),
        F.lower(F.col('medicinemanufacturer')).alias('medicinemanufacturer'),
        'practiceid'
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
        .withColumn('immunizationname', F.lower(F.col('immunizationname')))
        .transform(clean_date('immunizationstartdate'))
        .transform(clean_date('immunizationstopdate'))
        .transform(clean_date('administeredeffectivedate'))
        .withColumn('procedurecode', F.upper(_clean_input('procedurecode')))
    )
    
    return df

# COMMAND ----------

def load_patientprocedure_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.patientprocedure table."""
    
    df = spark.table("cdh_abfm_phi.patientprocedure")
    
    cols = [
        'patientuid',
        'procedurecode',
        'proceduredescription',
        'effectivedate',
        'serviceprovidernpi',
        'practiceid',
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
        .withColumn(a:='procedurecode', F.upper(_clean_input(F.col(a))))
        .withColumn(b:='proceduredescription', F.lower(F.col(b)))
        .withColumn(c:='serviceprovidernpi', F.when(F.col(c).rlike("\\d{10}"), F.col(c)))
        .transform(clean_date('effectivedate'))
        .filter(F.col('effectivedate').cast('date')<=F.to_date(F.current_date()))
    )
    
    return df

# COMMAND ----------

def load_patientlaborder_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.patientlaborder table."""
    
    df = spark.table("cdh_abfm_phi.patientlaborder")
    
    cols = [
        'patientuid',
        F.lower(F.col('description')).alias('description'),
        'orderdate',
        'practiceid'
    ] if subset else ['*']
    
    df = (
        df
        .select(*cols)
        .transform(clean_date('orderdate'))
    )
    
    return df

# COMMAND ----------

def load_patientlanguage_table(subset=True):
    """Returns a cleaned cdh_abfm_phi.patientlanguage table."""
    
    df = spark.table("cdh_abfm_phi.patientlanguage")
    
    cols = [
        'patientuid',
        'languagetext',
        'practiceid'
    ] if subset else ['*']

    df = (
        df
        .select(*cols)
        .transform(clean_languagetext('languagetext'))
        .groupBy('patientuid', 'languagetext')
        .count()
        .withColumn("count_", F.array("count", "languagetext"))
        .groupby("patientuid")
        .agg(
            F.max("count_").getItem(1).alias("languagetext"),
            F.when(
                F.count('patientuid')>1, 
                F.min("count_").getItem(1)
            ).alias("other_languagetext"),
        )
    )
    
    return df

# COMMAND ----------

# def load_{}_table():
#     """Returns a cleaned cdh_abfm_phi.{} table."""
    
#     df = spark.table("cdh_abfm_phi.{}")
    
#     df = (df)
    
#     return df
