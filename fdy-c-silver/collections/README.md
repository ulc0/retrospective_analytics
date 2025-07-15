### Core Collection Tables

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

### Collection Data

```mermaid
flowchart TD
subgraph relation
concept_code
vocabulary_id
end
subgraph Collection
  concept_code
  vocabulary_id
  collection_name
end
subgraph Facts
   patient_id
   episode_datetime 
   concept_code
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
        {{"`*sequence*`"}}
        v((vocabulary_id))
        ::icon(fa fa-book)
        c)concept_code(
        ::icon(fas fa-file-medical)
    collection_vector
        v((vocabulary_id))
        ::icon(fa fa-book)
        c)concept_code(
        ::icon(fas fa-file-medical)    
        {{"`*feature*`"}}
    vocabulary_set
        v((vocabulary_id))
        ::icon(fa fa-book)
        c)concept_code(
```

#### Canonical Data Fact Tables

#### Vocabulary Code Reference Table

#### Vocabulary Code Feature Dimensions
