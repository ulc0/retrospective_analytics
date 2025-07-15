#### Dates by event table
```R
dates = list(
        Account= c('ServiceStartDateTime', 'RecordedDateTime'), # note: not in Prose
        CareSiteRequestHistory= 'EffectiveDateTime', # note: not in Prose
        Claim= 'ServiceBeginDate', # note: not in Prose
        ChargeItem = c('ServiceDateTime', 'RecordedDateTime'), # note: not in Prose
        Condition= c('OnsetDateTime', 'RecordedDateTime'),
        DeviceUse= c('ActionDateTime', 'RecordedDateTime'),
        Encounter= 'StartDateTime',
        EncounterHistory= c('StartDateTime', 'RecordedDateTime'), # note: not in Prose
        Enrollment = 'StartDate',
        EventLog = c('EffectiveDateTime', 'RecordedDateTime'), # note: not in Prose
        Extract= c('EffectiveDateTime', 'RecordedDateTime'), # note: not in Prose
        FamilyMemberHistory= 'RecordedDateTime',
        ImagingInstance= 'AcquisitionDateTime',
        ImagingSeries= 'SeriesDateTime',
        ImagingStudy= 'StudyDateTime',
        Immunization= c('AdministeredDateTime', 'RecordedDateTime'),
        LabResult= c('EffectiveDateTime', 'SpecimenCollectionDateTime', 'RecordedDateTime'),
        MedicalClaim = 'ServiceStartDate',
        MedicationAdministration= c('StartDateTime', 'RecordedDateTime'),
        MedicationDispense= 'DispenseDateTime',
        MedicationRequest= c('StartDateTime', 'AuthoredOnDateTime'),
        MedicationStatement= c('EffectiveDateTime', 'RecordedDateTime'),
        Note= 'EffectiveDateTime',
        Observation= c('EffectiveDateTime', 'RecordedDateTime'),
        PersonDeathFact= 'DeathDateTime',
        PersonLocation= 'EffectiveStartDateTime', # note: in Prose it is EffectiveDateTime
        Person= 'BirthDateTime',
        PharmacyClaim= 'ServiceDate',
        Procedure= c('StartDateTime', 'RecordedDateTime'),
        ServiceRequest= c('OccurrenceStartDateTime', 'AuthoredOnDateTime', 'RecordedDateTime')
    )
```