Synthea can be configured to export data as CSV into `./output/csv`. Test

To export CSV, edit `./src/main/resources/synthea.properties` and change this setting:

```properties
exporter.csv.export = true
```

After running Synthea, the CSV exporter will create these files:

| File | Description |
|------|-------------|
| [`allergies.csv`](#allergies) | Patient allergy data. |
| [`careplans.csv`](#careplans) | Patient care plan data, including goals. |
| [`claims.csv`](#claims) | Patient claim data. |
| [`claims_transactions.csv`](#claims-transactions) | Transactions per line item per claim. |
| [`conditions.csv`](#conditions) | Patient conditions or diagnoses. |
| [`devices.csv`](#devices) | Patient-affixed permanent and semi-permanent devices. |
| [`encounters.csv`](#encounters) | Patient encounter data. |
| [`imaging_studies.csv`](#imaging-studies) | Patient imaging metadata. |
| [`immunizations.csv`](#immunizations) | Patient immunization data. |
| [`medications.csv`](#medications) | Patient medication data. |
| [`observations.csv`](#observations) | Patient observations including vital signs and lab reports. |
| [`organizations.csv`](#organizations) | Provider organizations including hospitals. |
| [`patients.csv`](#patients) | Patient demographic data. |
| [`payer_transitions.csv`](#payer-transitions) | Payer Transition data (i.e. changes in health insurance). |
| [`payers.csv`](#payers) | Payer organization data. |
| [`procedures.csv`](#procedures) | Patient procedure data including surgeries. |
| [`providers.csv`](#providers) | Clinicians that provide patient care. |
| [`supplies.csv`](#supplies) | Supplies used in the provision of care. |


Data Dictionary information for each CSV table follows below.

# Allergies
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Start | Date (`YYYY-MM-DD`) | `true` | The date the allergy was diagnosed. |
| | Stop | Date (`YYYY-MM-DD`) | `false` | The date the allergy ended, if applicable. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter when the allergy was diagnosed. |
| | Code | String | `true` | Allergy code |
| | System | String | `true` | Terminology system of the Allergy code. `RxNorm` if this is a medication allergy, otherwise `SNOMED-CT`. |
| | Description | String | `true` | Description of the Allergy |
| | Type | String | `false` | Identify entry as an `allergy` or `intolerance`. |
| | Category | String | `false` | Identify the category as `drug`, `medication`, `food`, or `environment`. |
| | Reaction1 | String | `false` | Optional SNOMED code of the patients reaction. |
| | Description1 | String | `false` | Optional description of the `Reaction1` SNOMED code. |
| | Severity1 | String | `false` | Severity of the reaction: `MILD`, `MODERATE`, or `SEVERE`. |
| | Reaction2 | String | `false` | Optional SNOMED code of the patients second reaction. |
| | Description2 | String | `false` | Optional description of the `Reaction2` SNOMED code. |
| | Severity2 | String | `false` | Severity of the second reaction: `MILD`, `MODERATE`, or `SEVERE`. |

# CarePlans
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary Key. Unique Identifier of the care plan. |
| | Start | Date (`YYYY-MM-DD`) | `true` | The date the care plan was initiated. |
| | Stop | Date (`YYYY-MM-DD`) | `false` | The date the care plan ended, if applicable. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter when the care plan was initiated. |
| | Code | String | `true` | Code from SNOMED-CT |
| | Description | String | `true` | Description of the care plan. |
| | ReasonCode | String | `true` | Diagnosis code from SNOMED-CT that this care plan addresses. |
| | ReasonDescription | String | `true` | Description of the reason code. |

# Claims
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary Key. Unique Identifier of the claim. |
| :old_key: | Patient ID | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Provider ID | UUID | `true` | Foreign key to the Provider. |
| :old_key: | Primary Patient Insurance ID | UUID | `false` | Foreign key to the primary Payer. |
| :old_key: | Secondary Patient Insurance ID | UUID | `false` | Foreign key to the second Payer. |
| | Department ID | Numeric | `true` | Placeholder for department. |
| | Patient Department ID | Numeric | `true` | Placeholder for patient department. |
| | Diagnosis1 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis2 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis3 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis4 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis5 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis6 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis7 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| | Diagnosis8 | String | `false` | SNOMED-CT code corresponding to a diagnosis related to the claim. |
| :old_key: | Referring Provider ID | UUID | `false` | Foreign key to the Provider who made the referral. |
| :old_key: | Appointment ID | UUID | `false` | Foreign key to the Encounter. |
| | Current Illness Date | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date the patient experienced symptoms. |
| | Service Date | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date of the services on the claim. |
| :old_key: | Supervising Provider ID | UUID | `false` | Foreign key to the supervising Provider. |
| | Status1 | String | `false` | Status of the claim from the Primary Insurance. `BILLED` or `CLOSED`. |
| | Status2 | String | `false` | Status of the claim from the Secondary Insurance. `BILLED` or `CLOSED`. |
| | StatusP | String | `false` | Status of the claim from the Patient. `BILLED` or `CLOSED`. |
| | Outstanding1 | Numeric | `false` | Total amount of money owed by Primary Insurance. |
| | Outstanding2 | Numeric | `false` | Total amount of money owed by Secondary Insurance. |
| | OutstandingP | Numeric | `false` | Total amount of money owed by Patient. |
| | LastBilledDate1 | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | Date the claim was sent to Primary Insurance. |
| | LastBilledDate2 | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | Date the claim was sent to Secondary Insurance. |
| | LastBilledDateP | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | Date the claim was sent to the Patient. |
| | HealthcareClaimTypeID1 | Numeric | `false` | Type of claim: `1` is professional, `2` is institutional. |
| | HealthcareClaimTypeID2 | Numeric | `false` | Type of claim: `1` is professional, `2` is institutional. |

# Claims Transactions
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary Key. Unique Identifier of the claim transaction. |
| :old_key: | Claim ID | UUID | `true` | Foreign key to the Claim. |
| | Charge ID | Numeric | `true` | Charge ID. |
| :old_key: | Patient ID | UUID | `true` | Foreign key to the Patient. |
| | Type | String | `true` | `CHARGE`: original line item. `PAYMENT`: payment made against a charge by an insurance company (aka Payer) or patient. `ADJUSTMENT`: change in the charge without a payment, made by an insurance company. `TRANSFERIN` and `TRANSFEROUT`: transfer of the balance from one insurance company to another, or to a patient. |
| | Amount | Numeric | `false` | Dollar amount for a `CHARGE` or `TRANSFERIN`. |
| | Method | String | `false` | Payment made by `CASH`, `CHECK`, `ECHECK`, `COPAY`, `SYSTEM` (adjustments without payment), or `CC` (credit card). |
| | From Date | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | Transaction start date. |
| | To Date | Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | Transaction end date. |
| :old_key: | Place of Service | UUID | `true` | Foreign key to the Organization. |
| | Procedure Code | String | `true` | SNOMED-CT or other code (e.g. CVX for Vaccines) for the service. |
| | Modifier1 | String | `false` | Unused. Modifier on procedure code. |
| | Modifier2 | String | `false` | Unused. Modifier on procedure code. |
| | DiagnosisRef1 | Numeric | `false` | Number indicating which diagnosis code from the claim applies to this transaction, 1-8 are valid options. |
| | DiagnosisRef2 | Numeric | `false` | Number indicating which diagnosis code from the claim applies to this transaction, 1-8 are valid options. |
| | DiagnosisRef3 | Numeric | `false` | Number indicating which diagnosis code from the claim applies to this transaction, 1-8 are valid options. |
| | DiagnosisRef4 | Numeric | `false` | Number indicating which diagnosis code from the claim applies to this transaction, 1-8 are valid options. |
| | Units | Numeric | `false` | Number of units of the service. |
| | Department ID | Numeric | `false` | Placeholder for department. |
| | Notes | String | `false` | Description of the service or transaction. |
| | Unit Amount | Numeric | `false` | Cost per unit. |
| | Transfer Out ID | Numeric | `false` | If the transaction is a `TRANSFERIN`, the Charge ID of the corresponding `TRANSFEROUT` row. |
| | Transfer Type | String | `false` | `1` if transferred to the primary insurance, `2` if transferred to the secondary insurance, or `p` if transferred to the patient. |
| | Payments | Numeric | `false` | Dollar amount of a payment for a `PAYMENT` row. |
| | Adjustments | Numeric | `false` | Dollar amount of an adjustment for an `ADJUSTMENTS` row. |
| | Transfers | Numeric | `false` | Dollar amount of a transfer for a `TRANSFERIN` or `TRANSFEROUT` row. |
| | Outstanding | Numeric | `false` | Dollar amount left unpaid after this transaction was applied. |
| :old_key: | Appointment ID | UUID | `false` | Foreign key to the Encounter. |
| | Line Note | String | `false` | Note. |
| :old_key: | Patient Insurance ID | UUID | `false` | Foreign key to the Payer Transitions table member ID. |
| | Fee Schedule ID | Numeric | `false` | Fixed to `1`. |
| :old_key: | Provider ID | UUID | `true` | Foreign key to the Provider. |
| :old_key: | Supervising Provider ID | UUID | `false` | Foreign key to the supervising Provider. |

# Conditions
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Start | Date (`YYYY-MM-DD`) | `true` | The date the condition was diagnosed. |
| | Stop | Date (`YYYY-MM-DD`) | `false` | The date the condition resolved, if applicable. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter when the condition was diagnosed. |
| | Code | String | `true` | Diagnosis code from SNOMED-CT |
| | Description | String | `true` | Description of the condition. |

# Devices
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Start | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the device was associated to the patient. |
| | Stop | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | The date and time the device was removed, if applicable. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter when the device was associated. |
| | Code | String | `true` | Type of device, from SNOMED-CT |
| | Description | String | `true` | Description of the device. |
| | UDI | String | `true` | [Unique Device Identifier](https://www.fda.gov/medical-devices/unique-device-identification-system-udi-system/udi-basics) for the device. |

# Encounters
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary Key. Unique Identifier of the encounter. |
| | Start | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the encounter started. |
| | Stop | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | The date and time the encounter concluded. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Organization | UUID | `true` | Foreign key to the Organization. |
| :old_key: | Provider | UUID | `true` | Foreign key to the Provider. |
| :old_key: | Payer | UUID | `true` | Foreign key to the Payer. |
| | EncounterClass | String | `true` | The class of the encounter, such as `ambulatory`, `emergency`, `inpatient`, `wellness`, or `urgentcare` |
| | Code | String | `true` | Encounter code from SNOMED-CT |
| | Description | String | `true` | Description of the type of encounter. |
| | Base_Encounter_Cost | Numeric | `true` | The base cost of the encounter, **not** including any line item costs related to medications, immunizations, procedures, or other services. |
| | Total_Claim_Cost | Numeric | `true` | The total cost of the encounter, including all line items. |
| | Payer_Coverage | Numeric | `true` | The amount of cost covered by the Payer. |
| | ReasonCode | String | `false` | Diagnosis code from SNOMED-CT, **only if** this encounter targeted a specific condition. |
| | ReasonDescription | String | `false` | Description of the reason code. |

# Imaging Studies
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Id | UUID | `true` | Non-unique identifier of the imaging study. An imaging study may have multiple rows. |
| | Date | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the imaging study was conducted. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter where the imaging study was conducted. 
| | Series UID | String | `true` | Imaging Study series DICOM UID. |
| | Body Site Code | String | `true` | A SNOMED Body Structures code describing what part of the body the images in the series were taken of. |
| | Body Site Description | String | `true` | Description of the body site. |
| | Modality Code | String | `true` | A [DICOM-DCM](https://github.com/synthetichealth/synthea/wiki/Generic-Module-Framework%3A-Basics#dicom-dcm-codes) code describing the method used to take the images. |
| | Modality Description | String | `true` | Description of the modality. |
| | Instance UID | String | `true` | Imaging Study instance DICOM UID. |
| | SOP Code | String | `true` | A [DICOM-SOP](https://github.com/synthetichealth/synthea/wiki/Generic-Module-Framework%3A-Basics#dicom-sop-codes) code describing the Subject-Object Pair (SOP) that constitutes the image. |
| | SOP Description | String | `true` | Description of the SOP code. |
| | Procedure Code | String | `true` | Procedure code from SNOMED-CT. |

# Immunizations
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Date | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date the immunization was administered. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter where the immunization was administered. 
| | Code | String | `true` | Immunization code from CVX. |
| | Description | String | `true` | Description of the immunization. |
| | Cost | Numeric | `true` | The line item cost of the immunization. |

# Medications
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Start | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the medication was prescribed. |
| | Stop | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | The date and time the prescription ended, if applicable. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Payer | UUID | `true` | Foreign key to the Payer. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter where the medication was prescribed. 
| | Code | String | `true` | Medication code from RxNorm. |
| | Description | String | `true` | Description of the medication. |
| | Base_Cost | Numeric | `true` | The line item cost of the medication. |
| | Payer_Coverage | Numeric | `true` | The amount covered or reimbursed by the Payer. |
| | Dispenses | Numeric | `true` | The number of times the prescription was filled. |
| | TotalCost | Numeric | `true` | The total cost of the prescription, including all dispenses. |
| | ReasonCode | String | `false` | Diagnosis code from SNOMED-CT specifying why this medication was prescribed. |
| | ReasonDescription | String | `false` | Description of the reason code. |

# Observations
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Date | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the observation was performed. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter where the observation was performed. |
| | Category | String | `false` | Observation category. |
| | Code | String | `true` | Observation or Lab code from LOINC |
| | Description | String | `true` | Description of the observation or lab. |
| | Value | String | `true` | The recorded value of the observation. |
| | Units | String | `false` | The units of measure for the value. |
| | Type | String | `true` | The datatype of `Value`: `text` or `numeric` |

# Organizations
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary key of the Organization. |
| | Name | String | `true` | Name of the Organization. |
| | Address | String | `true` | Organization's street address without commas or newlines. |
| | City | String | `true` | Street address city. |
| | State | String | `false` | Street address state abbreviation. |
| | Zip | String | `false` | Street address zip or postal code. |
| | Lat | Numeric | `false` | Latitude of Organization's address. |
| | Lon | Numeric | `false` | Longitude of Organization's address. |
| | Phone | String | `false` | Organization's phone number. |
| | Revenue | Numeric | `true` | The monetary revenue of the organization during the entire simulation. |
| | Utilization | Numeric | `true` | The number of Encounters performed by this Organization. |

# Patients
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary Key. Unique Identifier of the patient. |
| | BirthDate | Date (`YYYY-MM-DD`) | `true` | The date the patient was born. |
| | DeathDate | Date (`YYYY-MM-DD`) | `false` | The date the patient died. |
| | SSN | String | `true` | Patient Social Security identifier. |
| | Drivers | String | `false` | Patient Drivers License identifier. |
| | Passport | String | `false` | Patient Passport identifier. |
| | Prefix | String | `false` | Name prefix, such as `Mr.`, `Mrs.`, `Dr.`, etc. |
| | First | String | `true` | First name of the patient. |
| | Middle | String | `false` | Middle name of the patient. |
| | Last | String | `true` | Last or surname of the patient. |
| | Suffix | String | `false` | Name suffix, such as `PhD`, `MD`, `JD`, etc. |
| | Maiden | String | `false` | Maiden name of the patient. |
| | Marital | String | `false` | Marital Status. `M` is married, `S` is single. Currently no support for divorce (`D`) or widowing (`W`) |
| | Race | String | `true` | Description of the patient's primary race. |
| | Ethnicity | String | `true` | Description of the patient's primary ethnicity. |
| | Gender | String | `true` | Gender. `M` is male, `F` is female. |
| | BirthPlace | String | `true` | Name of the town where the patient was born. |
| | Address | String | `true` | Patient's street address without commas or newlines. |
| | City | String | `true` | Patient's address city. |
| | State | String | `true` | Patient's address state. |
| | County | String | `false` | Patient's address county. |
| | FIPS County Code | String | `false` | Patient's FIPS county code. |
| | Zip | String | `false` | Patient's zip code. |
| | Lat | Numeric | `false` | Latitude of Patient's address. |
| | Lon | Numeric | `false` | Longitude of Patient's address. |
| | Healthcare_Expenses | Numeric | `true` | The total lifetime cost of healthcare to the patient (i.e. what the patient paid). |
| | Healthcare_Coverage | Numeric | `true` | The total lifetime cost of healthcare services that were covered by Payers (i.e. what the insurance company paid). |
| | Income | Numeric | `true` | Annual income for the Patient |

# Payer Transitions
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| | Member ID | UUID | `false` | Member ID for the Insurance Plan. |
| | Start_Year | Date (`YYYY`) | `true` | The year the coverage started (inclusive). |
| | End_Year | Date (`YYYY`) | `true` | The year the coverage ended (inclusive). |
| :old_key: | Payer | UUID | `true` | Foreign key to the Payer. |
| :old_key: | Secondary Payer | UUID | `false` | Foreign key to the Secondary Payer. |
| | Ownership | String | `false` | The owner of the insurance policy. Legal values: `Guardian`, `Self`, `Spouse`. |
| | Owner Name | String | `false` | The name of the insurance policy owner. |

# Payers
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary key of the Payer (e.g. Insurance). |
| | Name | String | `true` | Name of the Payer. |
| | Address | String | `false` | Payer's street address without commas or newlines. |
| | City | String | `false` | Street address city. |
| | State_Headquartered | String | `false` | Street address state abbreviation. |
| | Zip | String | `false` | Street address zip or postal code. |
| | Phone | String | `false` | Payer's phone number. |
| | Amount_Covered | Numeric | `true` | The monetary amount paid to Organizations during the entire simulation. |
| | Amount_Uncovered | Numeric | `true` | The monetary amount not paid to Organizations during the entire simulation, and covered out of pocket by patients. |
| | Revenue | Numeric | `true` | The monetary revenue of the Payer during the entire simulation. |
| | Covered_Encounters | Numeric | `true` | The number of Encounters paid for by this Payer. |
| | Uncovered_Encounters | Numeric | `true` | The number of Encounters not paid for by this Payer, and paid out of pocket by patients. |
| | Covered_Medications | Numeric | `true` | The number of Medications paid for by this Payer. |
| | Uncovered_Medications | Numeric | `true` | The number of Medications not paid for by this Payer, and paid out of pocket by patients. |
| | Covered_Procedures | Numeric | `true` | The number of Procedures paid for by this Payer. |
| | Uncovered_Procedures | Numeric | `true` | The number of Procedures not paid for by this Payer, and paid out of pocket by patients. |
| | Covered_Immunizations | Numeric | `true` | The number of Immunizations paid for by this Payer. |
| | Uncovered_Immunizations | Numeric | `true` | The number of Immunizations not paid for by this Payer, and paid out of pocket by patients. |
| | Unique_Customers | Numeric | `true` | The number of unique patients enrolled with this Payer during the entire simulation. |
| | QOLS_Avg | Numeric | `true` | The average Quality of Life Scores (QOLS) for all patients enrolled with this Payer during the entire simulation. |
| | Member_Months | Numeric | `true` | The total number of months that patients were enrolled with this Payer during the simulation and paid monthly premiums (if any). |

# Procedures
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Start <br/> ("Date", prior to v3.0.0 ) | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `true` | The date and time the procedure was performed. |
| | Stop | iso8601 UTC Date (`yyyy-MM-dd'T'HH:mm'Z'`) | `false` | The date and time the procedure was completed, if applicable. 
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter where the procedure was performed. 
| | Code | String | `true` | Procedure code from SNOMED-CT |
| | Description | String | `true` | Description of the procedure. |
| | Base_Cost | Numeric | `true` | The line item cost of the procedure. |
| | ReasonCode | String | `false` | Diagnosis code from SNOMED-CT specifying why this procedure was performed. |
| | ReasonDescription | String | `false` | Description of the reason code. |

# Providers
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| :key: | Id | UUID | `true` | Primary key of the Provider/Clinician. |
| :old_key: | Organization | UUID | `true` | Foreign key to the Organization that employees this provider. |
| | Name | String | `true` | First and last name of the Provider. |
| | Gender | String | `true` | Gender. `M` is male, `F` is female. |
| | Speciality | String | `true` | Provider speciality. |
| | Address | String | `true` | Provider's street address without commas or newlines. |
| | City | String | `true` | Street address city. |
| | State | String | `false` | Street address state abbreviation. |
| | Zip | String | `false` | Street address zip or postal code. |
| | Lat | Numeric | `false` | Latitude of Provider's address. |
| | Lon | Numeric | `false` | Longitude of Provider's address. |
| | Utilization | Numeric | `true` | The number of Encounter's performed by this provider. |

# Supplies
| | Column Name | Data Type | Required? | Description |
|-|-------------|-----------|-----------|-------------|
| | Date | Date (`YYYY-MM-DD`) | `true` | The date the supplies were used. |
| :old_key: | Patient | UUID | `true` | Foreign key to the Patient. |
| :old_key: | Encounter | UUID | `true` | Foreign key to the Encounter when the supplies were used. |
| | Code | String | `true` | Code for the type of supply used, from SNOMED-CT |
| | Description | String | `true` | Description of supply used. |
| | Quantity | Numeric | `true` | Quantity of supply used. |