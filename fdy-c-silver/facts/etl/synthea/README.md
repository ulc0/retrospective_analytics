### Synthea CSV processing

* [Synthea CSV Data Dictionary](https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary)
* For Advanced Analytics only TimeStamps are converted from String

* Diagnosis
  - ReasonCode is always Diagnosis SNOMED-CT
  - Conditions are diagnoses CODE SNOMED-CT
  - 

* SNOMED-CT tables
  - Allergy (System, Reaction1, Reaction2)
  - CarePlans (Code, ReasonCode)
  - Claims (Diagnosis1-8)
  - ClaimsTransctions (ProcedureCode) 
  - Conditions (Code)
  - Devices (Code)
  - Encounter (Code, ReasonCode)
  - Imaging Studies (Body Site Code, Procedure Code)
  - Medication (ReasonCode)
  - Procedures (Code, ReasonCode)
  - Supplies (Code)

LOINC tables
  - Observations (Code)

RXNorm tables
  - Allergies (System)
  - Medications (Code)
  - Immunizations (CVX)