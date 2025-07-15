```mermaid
<<<<<<< HEAD
graph LR;
  subgraph orders TD
          servicerequest
 end
  subgraph diagnoses TD
          condition
     end
  subgraph observations TD
          observation
          observationcomponent
          labresult

end
  subgraph procedures TD
          procedure
          deviceuse
          immunization
end
  subgraph medications TD
          medicationrequest
          medicationdispense
          medicationadministration
          medicationstatement
          medications_bodysitemap
     end

     subgraph concepts TD  
          concept
          bodysitemap
          conditioncodeconceptmap
          observationcodeconceptmap
          labresultcodeconceptmap
          immunizationcodeconceptmap
          procedurecodeconceptmap
          servicerequestcodeconceptmap
          deviceusecodeconceptmap
          medicationcodeconceptmap
          medicationadherencereasonconceptmap
     end

     
     condition--> conditioncodeconceptmap 
     condition--> bodysitemap 
     deviceuse--> procedure 
     deviceuse--> deviceusecodeconceptmap 
     deviceuse--> bodysitemap 
     immunization--> immunizationcodeconceptmap 
     servicerequest-->labresult 
     labresult--> labresultcodeconceptmap 
     medicationadministration--> medicationrequest 
     medicationadministration--> medicationcodeconceptmap 
     medicationadministration--> bodysitemap 
     medicationdispense--> medicationcodeconceptmap 
     medicationrequest--> medicationcodeconceptmap 
     medicationstatement--> medicationadherencereasonconceptmap 
     medicationstatement--> medicationcodeconceptmap 
     medicationstatement--> bodysitemap 
     observation--> observationcodeconceptmap 
     observation--> bodysitemap 
     observationcomponent--> observation 
     servicerequest--> procedure 
     procedure--> procedurecodeconceptmap 
     procedure--> bodysitemap 
     servicerequest--> servicerequestcodeconceptmap

=======
graph TD
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
condition--encounterid-->encounter
condition--codeconceptmapid-->conditioncodeconceptmap
condition--bodysitemapid-->bodysitemap
deviceuse--personid-->person
deviceuse--encounterid-->encounter
deviceuse--procedureid-->procedure
deviceuse--codeconceptmapid-->deviceusecodeconceptmap
deviceuse--bodysitemapid-->bodysitemap
encounter--personid-->person
encounter--facilitylocationid-->location
encounter--caresiteid-->caresite
encounterhistory--personid-->person
encounterhistory--encounterid-->encounter
encounterhistory--caresiteid-->caresite
encounterparticipant--personid-->person
encounterparticipant--encounterid-->encounter
encounterparticipant--practitionerid-->practitioner
eventreference--personid-->person
extract--personid-->person
extract--extractschemaid-->extractschema
immunization--personid-->person
immunization--encounterid-->encounter
immunization--codeconceptmapid-->immunizationcodeconceptmap
labresult--personid-->person
labresult--encounterid-->encounter
labresult--servicerequestid-->servicerequest
labresult--codeconceptmapid-->labresultcodeconceptmap
medicationadministration--personid-->person
medicationadministration--encounterid-->encounter
medicationadministration--requestid-->medicationrequest
medicationadministration--codeconceptmapid-->medicationcodeconceptmap
medicationadministration--bodysitemapid-->bodysitemap
medicationdispense--personid-->person
medicationdispense--dispenserpractitionerid-->practitioner
medicationdispense--codeconceptmapid-->medicationcodeconceptmap
medicationrequest--personid-->person
medicationrequest--encounterid-->encounter
medicationrequest--requesterpractitionerid-->practitioner
medicationrequest--codeconceptmapid-->medicationcodeconceptmap
medicationstatement--personid-->person
medicationstatement--adherencereasonconceptmapid-->medicationadherencereasonconceptmap
medicationstatement--codeconceptmapid-->medicationcodeconceptmap
medicationstatement--encounterid-->encounter
medicationstatement--bodysitemapid-->bodysitemap
observation--personid-->person
observation--encounterid-->encounter
observation--codeconceptmapid-->observationcodeconceptmap
observation--bodysitemapid-->bodysitemap
observationcomponent--personid-->person
observationcomponent--observationid-->observation
practitionerqualification--practitionerid-->practitioner
procedure--personid-->person
procedure--encounterid-->encounter
procedure--servicerequestid-->servicerequest
procedure--codeconceptmapid-->procedurecodeconceptmap
procedure--bodysitemapid-->bodysitemap
servicerequest--personid-->person
servicerequest--encounterid-->encounter
servicerequest--requesterpractitionerid-->practitioner
servicerequest--codeconceptmapid-->servicerequestcodeconceptmap
```
>>>>>>> b1c88a5 (update)
