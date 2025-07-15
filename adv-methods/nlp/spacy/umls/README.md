### UMLS Detection and Linkage for Featurization

Unstructured Data includes Text Descriptions unlinked to codes, such as a LOINC description without a LOINC code:  

| LOINC Description | LOINC Code|
|-------------------|-----------|
|SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar||
|SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar||


[AllenAI](allenai.org)'s [SciSpacy](https://allenai.github.io/scispacy/) provides NLP models and [UMLS entity linkers](https://scispacy.apps.allenai.org/) to both identify coding and proper descriptors to standardize codes. SciSpacy models can be deployed within CDH Databricks as Model `Endpoints` for batch inference of code, as well as wrapped in a python function for deployment within a featurization pipeline.  

CDC DataHub can make use of this in canonical data sets that have descriptions only with no coding, such as Premier GenLab, and to identify uncoded umls medical concepts within unstructured text fields.
