### Core concept_set Tables

#### UMLS Value Sets
- [PHIN VADS](https://phinvads.cdc.gov/vads/SearchVocab.action)
- [UMLS MetaThesaurus API](https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/index.html)

Core difference is update schedule
| Table | [MetaThesaursus](https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/index.html)  | [PHIN VADS](https://phinvads.cdc.gov/vads/downloads/CodeSystem_UpdateCalendar.pdf) |
| --- | --- | --- |
| LOINC | bim | Jul,Dec |
| SNOMED | | Mar, Sep|
| RxNorm | | April |
| ICD-10 | | September|
| NNDSS | | December |
| HL7 | | June|

### concept_set Data

```mermaid
flowchart TD
subgraph relation
code
end
subgraph concept_sets
  code
  concept_set_name
end
subgraph Facts
   patient_id
   episode_datetime 
   code
   domain_id
   source
   vocabulary_id
end
relation-->Facts
relation-->Features
```


```mermaid

mindmap
root((features))
    facts 
        {{"`*person_id*`"}}
        {{"`*datetime*`"}}
        {{"`*event source*`"}}    
        c)code(
        ::icon(fas fa-file-medical)
        d)domain(
    concept_set_vector
        c)code(
        ::icon(fas fa-file-medical)    
        {{"`*concept_set*`"}}
```

#### Canonical Data Fact Tables

#### Vocabulary Code Reference Table

#### Vocabulary Code Feature Dimensions
