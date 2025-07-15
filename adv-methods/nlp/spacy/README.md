### [SciSpacy](https://allenai.github.io/scispacy/) from [AI2](https://allenai.org)  

SciSpacy is a [SpaCy](https://www.spacy.io) package which builds SpaCy NLP models with both an [Entity Linker](https://spacy.io/api/entitylinker/) and the SciSpacy [Abbreviation Detector](https://github.com/allenai/scispacy/blob/main/scispacy/abbreviation.py), which are accessed through SciSpacy [models]() 

#### Training format for entity linker
* ```TRAIN_DATA = ("Emerson was born on a farm in Blackbutt, Queensland.", {"links": {(0, 7): { "Q312545": 1.0 }}})```

