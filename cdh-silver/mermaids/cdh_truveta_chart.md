```mermaid
graph TD

subgraph account
     account.id
end
subgraph accountcoverage
     accountcoverage.id
end
subgraph accountencounter
     accountencounter.id
end
subgraph accountpaymentclassification
     accountpaymentclassification.id
end
subgraph bodysitemap
     bodysitemap.id
end
subgraph bodysitemaphistory
     bodysitemaphistory.id
end
subgraph caresite
     caresite.id
end
subgraph claim
     claim.id
end
subgraph claimline
     claimline.id
end
subgraph concept
     concept.conceptid
end
subgraph condition
     condition.id
end
subgraph conditioncodeconceptmap
     conditioncodeconceptmap.id
end
subgraph conditioncodeconceptmaphistory
     conditioncodeconceptmaphistory.id
end
subgraph coverage
     coverage.id
end
subgraph deviceuse
     deviceuse.id
end
subgraph deviceusecodeconceptmap
     deviceusecodeconceptmap.id
end
subgraph deviceusecodeconceptmaphistory
     deviceusecodeconceptmaphistory.id
end
subgraph encounter
     encounter.id
end
subgraph encounterhistory
     encounterhistory.id
end
subgraph encounterparticipant
     encounterparticipant.id
end
subgraph eventreference
     eventreference.id
end
subgraph extract
     extract.id
end
subgraph extractschema
     extractschema.extractschemaid
end
subgraph familymemberhistory
     familymemberhistory.id
end
subgraph familymemberhistorycodeconceptmap
     familymemberhistorycodeconceptmap.id
end
subgraph familymemberhistorycodeconceptmaphistory
     familymemberhistorycodeconceptmaphistory.id
end
subgraph familymemberhistoryoutcomeconceptmap
     familymemberhistoryoutcomeconceptmap.id
end
subgraph familymemberhistoryoutcomeconceptmaphistory
     familymemberhistoryoutcomeconceptmaphistory.id
end
subgraph imaginginstance
     imaginginstance.id
end
subgraph imagingproperty
     imagingproperty.id
end
subgraph imagingseries
     imagingseries.id
end
subgraph imagingstudy
     imagingstudy.id
end
subgraph immunization
     immunization.id
end
subgraph immunizationcodeconceptmap
     immunizationcodeconceptmap.id
end
subgraph immunizationcodeconceptmaphistory
     immunizationcodeconceptmaphistory.id
end
subgraph labresult
     labresult.id
end
subgraph labresultcodeconceptmap
     labresultcodeconceptmap.id
end
subgraph labresultcodeconceptmaphistory
     labresultcodeconceptmaphistory.id
end
subgraph location
     location.id
end
subgraph medicationadherencereasonconceptmap
     medicationadherencereasonconceptmap.id
end
subgraph medicationadherencereasonconceptmaphistory
     medicationadherencereasonconceptmaphistory.id
end
subgraph medicationadministration
     medicationadministration.id
end
subgraph medicationcodeconceptmap
     medicationcodeconceptmap.id
end
subgraph medicationcodeconceptmaphistory
     medicationcodeconceptmaphistory.id
end
subgraph medicationdispense
     medicationdispense.id
end
subgraph medicationrequest
     medicationrequest.id
end
subgraph medicationstatement
     medicationstatement.id
end
subgraph note
     note.id
end
subgraph observation
     observation.id
end
subgraph observationcodeconceptmap
     observationcodeconceptmap.id
end
subgraph observationcodeconceptmaphistory
     observationcodeconceptmaphistory.id
end
subgraph observationcomponent
     observationcomponent.id
end
subgraph patient
     patient.id
end
subgraph person
     person.id
end
subgraph persondeathfact
     persondeathfact.id
end
subgraph personepctlink
     personepctlink.personid
end
subgraph personlocation
     personlocation.id
end
subgraph personrace
     personrace.id
end
subgraph personrelationship
     personrelationship.id
end
subgraph persontag
     persontag.id
end
subgraph practitioner
     practitioner.id
end
subgraph practitionerqualification
     practitionerqualification.id
end
subgraph procedure
     procedure.id
end
subgraph procedurecodeconceptmap
     procedurecodeconceptmap.id
end
subgraph procedurecodeconceptmaphistory
     procedurecodeconceptmaphistory.id
end
subgraph servicerequest
     servicerequest.id
end
subgraph servicerequestcodeconceptmap
     servicerequestcodeconceptmap.id
end
subgraph servicerequestcodeconceptmaphistory
     servicerequestcodeconceptmaphistory.id
end
subgraph socialdeterminantsofhealth
     socialdeterminantsofhealth.id
end
subgraph temporalorder
     temporalorder.id
end
account--personid-->person
account--patientid-->patient
accountcoverage--personid-->person
accountcoverage--patientid-->patient
accountcoverage--accountid-->account
accountcoverage--coverageid-->coverage
accountencounter--personid-->person
accountencounter--patientid-->patient
accountencounter--accountid-->account
accountencounter--primaryencounterid-->encounter
accountencounter--encounterid-->encounter
accountpaymentclassification--personid-->person
accountpaymentclassification--accountid-->account
caresite--locationid-->location
caresite--parentcaresiteid-->caresite
claim--personid-->person
claimline--personid-->person
claimline--claimid-->claim
claimlinecodes--personid-->person
claimlinecodes--claimlineid-->claimline
conceptlink--conceptid1-->concept
conceptlink--conceptid2-->concept
condition--personid-->person
condition--patientid-->patient
condition--encounterid-->encounter
condition--codeconceptmapid-->conditioncodeconceptmap
condition--bodysitemapid-->bodysitemap
deviceuse--personid-->person
deviceuse--patientid-->patient
deviceuse--encounterid-->encounter
deviceuse--procedureid-->procedure
deviceuse--codeconceptmapid-->deviceusecodeconceptmap
deviceuse--bodysitemapid-->bodysitemap
encounter--personid-->person
encounter--patientid-->patient
encounter--facilitylocationid-->location
encounter--caresiteid-->caresite
encounterhistory--personid-->person
encounterhistory--patientid-->patient
encounterhistory--encounterid-->encounter
encounterhistory--caresiteid-->caresite
encounterparticipant--personid-->person
encounterparticipant--encounterid-->encounter
encounterparticipant--practitionerid-->practitioner
eventreference--personid-->person
extract--personid-->person
extract--extractschemaid-->extractschema
familymemberhistory--personid-->person
familymemberhistory--patientid-->patient
familymemberhistory--encounterid-->encounter
familymemberhistory--outcomeconceptmapid-->familymemberhistoryoutcomeconceptmap
familymemberhistory--codeconceptmapid-->familymemberhistorycodeconceptmap
imaginginstance--personid-->person
imaginginstance--seriesid-->imagingseries
imagingproperty--personid-->person
imagingproperty--eventrecordid-->imagingstudy
imagingproperty--eventrecordid--> imagingseries
imagingproperty--eventrecordid--> imaginginstance
imagingseries--personid-->person
imagingseries--studyid-->imagingstudy
imagingstudy--personid-->person
imagingstudy--patientid-->patient
imagingstudy--encounterid-->encounter
imagingstudy--servicerequestid-->servicerequest
imagingstudy--noteid-->note
immunization--personid-->person
immunization--patientid-->patient
immunization--encounterid-->encounter
immunization--codeconceptmapid-->immunizationcodeconceptmap
labresult--personid-->person
labresult--patientid-->patient
labresult--encounterid-->encounter
labresult--servicerequestid-->servicerequest
labresult--codeconceptmapid-->labresultcodeconceptmap
medicationadministration--personid-->person
medicationadministration--patientid-->patient
medicationadministration--encounterid-->encounter
medicationadministration--requestid-->medicationrequest
medicationadministration--codeconceptmapid-->medicationcodeconceptmap
medicationadministration--bodysitemapid-->bodysitemap
medicationdispense--personid-->person
medicationdispense--patientid-->patient
medicationdispense--dispenserpractitionerid-->practitioner
medicationdispense--codeconceptmapid-->medicationcodeconceptmap
medicationrequest--personid-->person
medicationrequest--patientid-->patient
medicationrequest--encounterid-->encounter
medicationrequest--requesterpractitionerid-->practitioner
medicationrequest--codeconceptmapid-->medicationcodeconceptmap
medicationstatement--personid-->person
medicationstatement--patientid-->patient
medicationstatement--adherencereasonconceptmapid-->medicationadherencereasonconceptmap
medicationstatement--codeconceptmapid-->medicationcodeconceptmap
medicationstatement--encounterid-->encounter
medicationstatement--bodysitemapid-->bodysitemap
note--personid-->person
note--patientid-->patient
note--encounterid-->encounter
note--servicerequestid-->servicerequest
note--authorpractitionerid-->practitioner
observation--personid-->person
observation--patientid-->patient
observation--encounterid-->encounter
observation--codeconceptmapid-->observationcodeconceptmap
observation--bodysitemapid-->bodysitemap
observationcomponent--personid-->person
observationcomponent--patientid-->patient
observationcomponent--observationid-->observation
patient--personid-->person
persondeathfact--personid-->person
personlocation--personid-->person
personlocation--locationid-->location
personrace--personid-->person
personrelationship--personid-->person
personrelationship--patientid-->patient
personrelationship--relatedpatientid-->patient
personrelationship--relatedpersonid-->person
persontag--personid-->person
practitionerqualification--practitionerid-->practitioner
procedure--personid-->person
procedure--patientid-->patient
procedure--encounterid-->encounter
procedure--servicerequestid-->servicerequest
procedure--codeconceptmapid-->procedurecodeconceptmap
procedure--bodysitemapid-->bodysitemap
servicerequest--personid-->person
servicerequest--patientid-->patient
servicerequest--encounterid-->encounter
servicerequest--requesterpractitionerid-->practitioner
servicerequest--codeconceptmapid-->servicerequestcodeconceptmap
socialdeterminantsofhealth--personid-->person
temporalorder--personid-->person
=======
subgraphaccount
end
subgraphaccountcoverage
end
subgraphaccountencounter
end
subgraphaccountpaymentclassification
end
subgraphbodysitemap
end
subgraphbodysitemaphistory
end
subgraphcaresite
end
subgraphclaim
end
subgraphclaimline
end
subgraphclaimlinecodes
end
subgraphconcept
end
subgraphconceptlink
end
subgraphcondition
end
subgraphconditioncodeconceptmap
end
subgraphconditioncodeconceptmaphistory
end
subgraphcoverage
end
subgraphdeviceuse
end
subgraphdeviceusecodeconceptmap
end
subgraphdeviceusecodeconceptmaphistory
end
subgraphencounter
end
subgraphencounterhistory
end
subgraphencounterparticipant
end
subgrapheventreference
end
subgraphextendedproperty
end
subgraphextract
end
subgraphextractschema
end
subgraphfamilymemberhistory
end
subgraphfamilymemberhistorycodeconceptmap
end
subgraphfamilymemberhistorycodeconceptmaphistory
end
subgraphfamilymemberhistoryoutcomeconceptmap
end
subgraphfamilymemberhistoryoutcomeconceptmaphistory
end
subgraphimaginginstance
end
subgraphimagingproperty
end
subgraphimagingseries
end
subgraphimagingstudy
end
subgraphimmunization
end
subgraphimmunizationcodeconceptmap
end
subgraphimmunizationcodeconceptmaphistory
end
subgraphlabresult
end
subgraphlabresultcodeconceptmap
end
subgraphlabresultcodeconceptmaphistory
end
subgraphlocation
end
subgraphmedicationadherencereasonconceptmap
end
subgraphmedicationadherencereasonconceptmaphistory
end
subgraphmedicationadministration
end
subgraphmedicationcodeconceptmap
end
subgraphmedicationcodeconceptmaphistory
end
subgraphmedicationdispense
end
subgraphmedicationrequest
end
subgraphmedicationstatement
end
subgraphnote
end
subgraphobservation
end
subgraphobservationcodeconceptmap
end
subgraphobservationcodeconceptmaphistory
end
subgraphobservationcomponent
end
subgraphpatient
end
subgraphperson
end
subgraphpersondeathfact
end
subgraphpersonepctlink
end
subgraphpersonlocation
end
subgraphpersonrace
end
subgraphpersontag
end
subgraphpractitioner
end
subgraphpractitionerqualification
end
subgraphprocedure
end
subgraphprocedurecodeconceptmap
end
subgraphprocedurecodeconceptmaphistory
end
subgraphsearchresult_dob
end
subgraphservicerequest
end
subgraphservicerequestcodeconceptmap
end
subgraphservicerequestcodeconceptmaphistory
end
subgraphsocialdeterminantsofhealth
end
subgraphtemporalorder
end
```
