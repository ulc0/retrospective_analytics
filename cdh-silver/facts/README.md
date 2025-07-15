# Fact and concept_set Tables

**FEATURE_STORE** is inner join of Cohort-Scope, concept_set Name, Person Fact, and Provider Fact tables.

Using [Standardized Vocabularies](https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html) and custom CDH vocabularies e.g. edav_prd_cdh.cdh_PREMIER.CHGMSTR

* Premier
* ABFM
* MIMIC
* Synthea
* Truveta

### Patient Fact Tables

* Based on [OHDSI OMOP CDM 5.4](https://build.fhir.org/ig/HL7/fhir-omop-ig/cdm54.png)* at the [OBSERVATION](https://ohdsi.github.io/CommonDataModel/cdm54.html)  
 level
  * person_id - unique patient identifer
  * [visit_occurrence_number](https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_period)  
  * [visit_start_date](https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_period)  
  * [code](https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT)  
  * [vocabulary_id](https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY) -- Assigned from CONCEPT table  
  * [domain_id](https://ohdsi.github.io/CommonDataModel/cdm54.html#DOMAIN) -- Assigned from CONCEPT table  

    Across all event types

### Provider Fact Tables

Same as Patient Fact table, with person_id as provider code

### concept_set Name Table

* code
* vocabulary_id
* concept_set_name
* [OID](https://www.cdc.gov/nhsn/cdaportal/sds/oid.html)

Cohort Table/List **defined by Principal Investigator*

* person_id

### [OMOP ERD](https://ohdsi.github.io/CommonDataModel/cdm54erd.html)

![OMOP Sources](https://build.fhir.org/ig/HL7/fhir-omop-ig/cdm54.png)

[Full Model](https://ohdsi.github.io/CommonDataModel/images/erd.jpg)
