### [MIT's MIMIC (Medical Information Mart for Intensive Care)](https://mimic.mit.edu/)
* [AWS Access](https://aws.amazon.com/blogs/big-data/perform-biomedical-informatics-without-a-database-using-mimic-iii-data-and-amazon-athena/)
* [AWS Link](https://registry.opendata.aws/mimiciii/)
* [A project of MIT Laboratory for Computation Physiol(ogy](https://lcp.mit.edu/mimic)
* [MIMIC-III](https://mimic.mit.edu/docs/iii/) is a [standard](https://link.springer.com/chapter/10.1007/978-3-030-77211-6_12) for Bio-Medical NLP & LLM models
* [MIMIC-IV](https://mimic.mit.edu/docs/iv/) has been released
    - Five modules
        * [notes](https://physionet.org/content/mimic-iv-note/2.2/)        
        * [hosp](https://mimic.mit.edu/docs/iv/modules/hosp) - hospital level labs, micro, electronic meds
        * [icu](https://mimic.mit.edu/docs/iv/modules/icu) - icu events
        * [ed](https://mimic.mit.edu/docs/iv/modules/ed) - emergency events
        * [cxr](https://mimic.mit.edu/docs/iv/modules/cxr) - cross reference data
* hosp, icu, ed, and cxr are publicly available
    - CDH AI/AA would like to load this data into the AI/ML Silver Medallion Format for Advanced Analytics use
    - We see no use case that the "Bronze" Medallion enhanced level be maintained in the Delta Lake
        * this follows the pattern "synthea" generated data downloaded for SNOMED-CT Featurization 
* MIMIC-IV Notes data has a credentialing process that requires one to 
    - be a PhysioNet Credentialed User
    - complete [CITI Data or Specimens Only Research](https://physionet.org/content/mimic-iv-note/view-required-training/2.2/#1) Training
    - [submit](https://physionet.org/settings/training/) proof of completion of that training
    - sign the [data use agreement](https://physionet.org/sign-dua/mimic-iv-note/2.2/)
    - CDH could require the proof of completion and data use agreement as an onboarding to Notes