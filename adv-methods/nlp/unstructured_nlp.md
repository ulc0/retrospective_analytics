```mermaid
flowchart TD
subgraph NLP
   vocabulary_id
   concept_code
end
subgraph Features
  concept_code
  vocabulary_id
  feature
end
subgraph Unstructured
   patient_id
   episode_datetime
   episode_number
   hospital_time_index
   text
end
subgraph TimeSeries
   Unstructured
   NLP
end
text-->NLP
NLP-->Features
```
