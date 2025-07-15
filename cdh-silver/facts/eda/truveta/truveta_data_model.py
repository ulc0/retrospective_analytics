# Databricks notebook source
# MAGIC %md
# MAGIC ### Results
# MAGIC
# MAGIC | personid                             | IDDA | IDDB | IDDC | IDDD | IDDE | IDD |
# MAGIC |--------------------------------------|------|------|------|------|------|-----|
# MAGIC | 931d8613-cf03-901f-48fa-ac2b2dea010c | 1    | 0    | 1    | 1    | 0    | 1   |
# MAGIC | 8f076556-4dfa-262e-f7f0-c51842bf9908 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 6dc473db-f5bf-9870-3477-718fc74f6e8c | 0    | 1    | 0    | 1    | 0    | 1   |
# MAGIC | 32f88e68-8051-f948-8f45-57434a7bf666 | 0    | 1    | 0    | 0    | 0    | 1   |
# MAGIC | 29b3fe60-5401-a47c-e234-020cd444263c | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | 26140bc6-c21a-ca73-fdcb-e35ccb72e651 | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | aa7a2344-94e9-3a45-dedd-d140eda32de0 | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | a176c3bc-17d1-5ee3-e6af-6cd5239f73fb | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | b15bc493-29b5-0e97-ce1a-6bae09cf5d59 | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | 5655d96e-c10f-eb82-7220-48316df9326a | 0    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | 27ff32f7-10f6-66a7-aa19-2c6dbd0a8521 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | ea977469-3f99-30d2-501b-253910a6ebc2 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | d1ca15b7-4a3c-5ede-bae3-6e73c5c552f3 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 698e06d8-418f-3025-a033-a17edf967497 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 48a1a9c1-daeb-238e-baa3-277ec9387324 | 1    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 1f6eb8a8-4d43-6534-69b0-df9516b2b332 | 1    | 0    | 0    | 1    | 0    | 1   |
# MAGIC | 73d0c244-840d-90e9-1149-406a4b7c53d6 | 1    | 0    | 1    | 0    | 1    | 1   |
# MAGIC | 52b6256d-bd62-8f11-6b03-a283e591e3a3 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 1d7cf263-177e-3d16-cf4a-dd2dd672d16d | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 7c81acd7-2144-0b8f-ca00-1da3b765e685 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 2930953c-6002-18f1-5c91-aeb886bbb698 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 9c38ee8b-2431-e403-a22d-02aeaf6d2559 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 6c35ee5b-4e67-0cfe-d143-ed4b696c34d2 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | c82aaef0-abe4-21b9-d3fe-2ce8939b2874 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 95163563-f55c-beb3-d0f3-7208d379acf1 | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 28192e26-6a5d-6025-3005-618f5db321ba | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 57e85264-ee92-54e0-fe66-394d33b897de | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 8ff0ce4b-0308-cd02-5aa8-8c60c0ccaa8a | 0    | 0    | 1    | 0    | 0    | 1   |
# MAGIC | 74d7b94f-dfb2-65f3-f0c2-a8c242bf7f72 | 0    | 0    | 1    | 1    | 0    | 1   |
# MAGIC | b0eccbd4-9de0-17bd-b986-4668aee57651 | 1    | 0    | 0    | 0    | 0    | 1   |
# MAGIC | 1ba3ca6f-de6a-f467-2b3b-28c1d8597012 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 6457ad02-1857-fa1f-46ca-4ba909740d21 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 4115fba5-49c9-ce57-9df2-26f499387d76 | 0    | 0    | 0    | 0    | 1    | 1   |
# MAGIC | 40c5dc50-786d-99dd-7a4a-845cf50e8841 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | ca3f4bf8-f5d8-1dac-7079-6b2af6174f8d | 0    | 1    | 0    | 0    | 1    | 1   |
# MAGIC | c23b6e20-adca-d6ee-3b09-92216da5a740 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 132a768e-f8a0-984d-f308-8640181da14c | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 2acd1300-74e2-6e90-71a2-82f081107931 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | abb5882a-099f-684a-329f-537e0b10c90f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 3baf8e30-c343-3345-d2c8-317d4bb8bd55 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 8512a673-d4c3-454e-7989-ad065ad29ea5 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | c4edf63f-b876-22f6-675b-87ccb206b321 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 4df9c2a1-7e47-3484-9356-9d70048e016f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 2d07df06-80d8-559a-d2b0-2050247b770b | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 5cf0f36c-5bff-5cd5-c077-d4b29c216988 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 5ad77044-cac5-611d-68c6-c11fc4e2c117 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 20f6c9e4-f7ea-9336-8091-11aebb486abf | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | fd2c579e-f494-2244-a07c-48c31782240a | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 154f3046-c702-8572-1053-ed27fefe8de6 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 1c94da13-ef53-64ac-51d9-3743ec72634b | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | b8c9a487-8cc2-0738-ac06-ed6c775ecc39 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 56154d3e-701d-8f60-1c98-4c17703d98ee | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | db6cdf13-e81f-e312-9cb8-cbdb4705b23b | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 2742a429-5fff-4f5b-8f94-4cf6e82a72d5 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 44786f8e-754e-4bc6-b12c-62f5e318550a | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | ad778485-5c7f-ac31-2ae2-1601deb29b18 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 29b38bbc-c916-8b4c-d79f-147eb4d81d6b | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 3691370f-571f-4d99-7e50-c314b6be3b45 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 5bea9a81-ca94-a523-61bf-b03292018847 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 3dbf5729-9fa7-7954-d267-b446ac88767b | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 4570204c-b5c3-bb62-5f04-81d8b3f96e66 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 90551627-10b5-3c9f-9bed-d6756f2987b0 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | e3ad5409-8a96-451a-f5a7-9159f3eb32b6 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | f468e196-1000-5469-2419-4743e469584f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 7f18b92c-fa61-c5c8-b03b-c0ead1057211 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | a3ba8bb4-808a-3275-9932-4c9859aa57c3 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 7469eefc-6ef7-c2b9-8e2f-4fed6edb157d | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 4457963b-28d5-e7f7-43c9-d3997c45e254 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 02a936a3-1181-eaa9-a660-67cb1c7407d0 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 0ec3bb9c-7381-43a7-4348-6e59298792d5 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 19799386-5dd1-e979-6f3a-9eafe0136dfc | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 947a0381-4744-b5b5-a2ec-92b6a8951abd | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | e246a78e-6169-0eca-937b-17627993d818 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 89f6d0fb-8b16-4e6e-6113-3a11decc55e3 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 7245d9b2-30e9-2e17-8a26-43049c267261 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 9dbbd186-1720-a2ab-135a-d3b3d15ef29a | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | eead3827-cb39-250e-599b-a76543af7000 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 41f5764b-3f17-9bca-a69f-b211d80b8cef | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 8efa6c4f-090f-44da-68d2-0d9b19db474f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 0039d4af-7781-58b3-1ed6-dabfbb886445 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 4bce7c6b-834d-9900-3767-0734906182b5 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 9f598d32-e2ac-29e5-4c9d-35cf74a30cfb | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 9fffa790-5bfc-4f39-8086-e9b7aaed6bda | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | f9bb9345-f695-4cb2-4941-18b8e8d1dedd | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 6f762e2d-b66c-2732-b0a5-c5c11cc6865f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 40ab3e8f-7df4-35d4-e31f-525358bc6ae7 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | c2f726e6-21d9-30b2-cf5d-2dfd9d27679f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 05deae0e-72c1-b99d-512d-5ad64b982bb6 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 23ad75f9-dcac-2743-edf7-6ce6b158423d | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | e1a2c9cf-60f4-7f5b-d3b9-0f7cc2bf3e20 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | f30751ff-af3a-5973-df21-472c05c6b40f | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 5a4a3b01-00c0-b004-cec7-62bb087b2212 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 66b7dd85-931e-940e-3e8c-969a50ff256d | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | f1279f68-8c39-21cd-5fd2-c2ea64ce9e0c | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 22e4b779-2335-548b-fa8c-81aad05af6e0 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | d26bbc3b-2784-1694-d2df-9bf6bc26ca4d | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | efc985f2-b82d-e470-5e51-b33e76935b93 | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 4ca89973-e428-2f8f-e479-d8b71a74b95c | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | 445ad897-105a-542f-8b77-4e117fe1d0de | 0    | 0    | 0    | 0    | 0    | 0   |
# MAGIC | c7bab524-93eb-39a6-9c89-30154f330dba | 0    | 0    | 0    | 0    | 0    | 0   |

# COMMAND ----------

mapDict = {
    "IDDA": [
        "Q87.11",
        "Q90.0",
        "Q90.1",
        "Q90.2",
        "Q90.9",
        "Q91.0",
        "Q91.1",
        "Q91.2",
        "Q91.3",
        "Q91.4",
        "Q91.5",
        "Q91.6",
        "Q91.7",
        "Q92.0",
        "Q92.1",
        "Q92.2",
        "Q92.5",
        "Q92.62",
        "Q92.7",
        "Q92.8",
        "Q92.9",
        "Q93.0",
        "Q93.1",
        "Q93.2",
        "Q93.3",
        "Q93.4",
        "Q93.5",
        "Q93.51",
        "Q93.59",
        "Q93.7",
        "Q93.81",
        "Q93.88",
        "Q93.89",
        "Q93.9",
        "Q95.2",
        "Q95.3",
        "Q99.2",
    ],
    "IDDB": ["G80.0", "G80.1", "G80.2", "G80.3", "G80.4", "G80.8", "G80.9"],
    "IDDC": ["F84.0", "F84.1", "F84.2", "F84.3", "F84.4", "F84.5", "F84.8", "F84.9"],
    "IDDD": ["Q86.0", "F70", "F71", "F72", "F73", "F78", "F78.A1", "F78.A9", "F79"],
    "IDDE": [
        "F82",
        "F88",
        "F89",
        "G31.81",
        "M95.2",
        "Q04.0",
        "Q04.1",
        "Q04.2",
        "Q04.3",
        "Q04.4",
        "Q04.5",
        "Q04.6",
        "Q04.8",
        "Q04.9",
        "P04.3",
        "Q67.6",
        "Q76.7",
        "Q67.8",
        "Q68.1",
        "Q74.3",
        "Q70.00",
        "Q70.01",
        "Q70.02",
        "Q70.03",
        "Q70.10",
        "Q70.11",
        "Q70.12",
        "Q70.13",
        "Q70.20",
        "Q70.21",
        "Q70.22",
        "Q70.23",
        "Q70.30",
        "Q70.31",
        "Q70.32",
        "Q70.33",
        "Q70.4",
        "Q70.9",
        "Q85.1",
        "Q87.1",
        "Q87.19",
        "Q87.2",
        "Q87.3",
        "Q87.5",
        "Q87.81",
        "Q87.82",
        "Q87.89",
        "Q89.7",
        "Q89.8",
        "R41.83",
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC map "condition" (diagnosis) to one of four IDD (A-D) with an overall code
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * Left Join of
# MAGIC   - **event table** condition c 
# MAGIC   - with **concept mapping table** conditioncodeconceptmap m
# MAGIC   - concept ct
# MAGIC * on
# MAGIC   - c.codeconceptmapid=m.id  
# MAGIC   - m.codeconceptid=ct.conceptid 
# MAGIC
# MAGIC to map p.personid to ct.conceptcode
# MAGIC
# MAGIC mapping ct.conceptcode to "feature" 
# MAGIC
# MAGIC ```mermaid
# MAGIC | conditioncodeconceptmap |     |
# MAGIC |-------------------------|-----|
# MAGIC | Id                      | int |
# MAGIC | CodeConceptId           | int |
# MAGIC | SourceConceptId         | int |
# MAGIC
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Databricks notebook source 3224318295639181
# MAGIC #Generate All Patients with Diagnosis IDD=1 and type of diagnosis, and no diagnosis IDD=0 from CONDITION TABLE 
# MAGIC t=spark.sql(" select p.personid,
# MAGIC             coalesce(IDDA,0) as IDDA,coalesce(IDDB,0) as IDDB,coalesce(IDDC,0) as IDDC
# MAGIC             ,coalesce(IDDD,0) as IDDD,coalesce(IDDE,0) as IDDE, greatest(coalesce(IDDA,0)
# MAGIC             ,coalesce(IDDB,0),coalesce(IDDC,0),coalesce(IDDD,0),coalesce(IDDE,0) ) 
# MAGIC             as IDD from 
# MAGIC             ( select distinct c.personid, 
# MAGIC             case  when ct.conceptcode  
# MAGIC             in ('Q87.11','Q90.0','Q90.1','Q90.2','Q90.9','Q91.0','Q91.1','Q91.2',
# MAGIC             'Q91.3','Q91.4','Q91.5','Q91.6','Q91.7','Q92.0','Q92.1','Q92.2',
# MAGIC             'Q92.5','Q92.62','Q92.7','Q92.8','Q92.9','Q93.0','Q93.1','Q93.2',
# MAGIC             'Q93.3','Q93.4','Q93.5','Q93.51','Q93.59','Q93.7','Q93.81',
# MAGIC             'Q93.88','Q93.89','Q93.9','Q95.2','Q95.3','Q99.2') then 'IDDA'   
# MAGIC             when ct.conceptcode in ('G80.0','G80.1','G80.2','G80.3','G80.4','G80.8','G80.9') then 'IDDB'  
# MAGIC             when ct.conceptcode in ('F84.0','F84.1','F84.2','F84.3','F84.4','F84.5','F84.8','F84.9') then 'IDDC'  
# MAGIC             when ct.conceptcode in ('Q86.0','F70','F71','F72','F73','F78','F78.A1','F78.A9','F79') then 'IDDD'  
# MAGIC             when ct.conceptcode in ('F82','F88','F89','G31.81','M95.2', 'Q04.0', 'Q04.1', 'Q04.2', 
# MAGIC             'Q04.3','Q04.4', 'Q04.5', 'Q04.6', 'Q04.8', 'Q04.9','P04.3','Q67.6', 
# MAGIC             'Q76.7', 'Q67.8', 'Q68.1', 'Q74.3','Q70.00', 'Q70.01', 'Q70.02', 'Q70.03',
# MAGIC              'Q70.10', 'Q70.11', 'Q70.12', 'Q70.13', 'Q70.20', 'Q70.21', 'Q70.22', 'Q70.23', 
# MAGIC              'Q70.30', 'Q70.31', 'Q70.32', 'Q70.33', 'Q70.4', 'Q70.9','Q85.1','Q87.1','Q87.19',
# MAGIC              'Q87.2','Q87.3','Q87.5','Q87.81','Q87.82','Q87.89','Q89.7','Q89.8','R41.83') then 'IDDE'  
# MAGIC              end as IDD 
# MAGIC              from cdh_truveta.condition c 
# MAGIC              --All pts,all diagnosis in universal dx table 
# MAGIC              left join cdh_truveta.conditioncodeconceptmap m 
# MAGIC              on c.codeconceptmapid=m.id 
# MAGIC              left join cdh_truveta.concept ct 
# MAGIC              on m.codeconceptid=ct.conceptid  ) as p  
# MAGIC              pivot (       count(1)      for IDD in ('IDDA','IDDB','IDDC','IDDD','IDDE') )  ") 
# MAGIC              t.write.mode( "overwrite ").saveAsTable( "cdh_sandbox.tog0_truveta_dgxp_ "+RunID)","# Generate table of All TDM primary Dianosis ICD10 codes and their corresponding CCSR categories occuring in 2016-2023 in inpatient encounters, for combined cases and control prdx=spark.sql("  with prydgx as (select personid,              ct.conceptcode as prydxicd10,ct.conceptname as prydxname              from cdh_truveta.condition c               left join cdh_truveta.conditioncodeconceptmap m on c.codeconceptmapid=m.id              left join cdh_truveta.concept ct on m.codeconceptid=ct.conceptid               left join cdh_truveta.concept ctc on c.primarydiagnosisconceptid=ctc.conceptid             where 1=1             and ctc.conceptname='Yes' --Primary diagnosis             and ct.codesystem in ('ICD10CM','ICD10PCS')             and recordeddatetime between '2016-01-01' and '2023-12-31'             and c.encounterid in (select id from cdh_truveta_exploratory.tog0_truveta_encounterv2 where clr='inp') --Encounters are inpatient encounters            )             --Identify top 10 diagnosis and ccsr among cases and control, combined.            select personid,prydxicd10,prydxname,CCSR_Category,CCSR_Category_Description             from prydgx dx            left join (select `ICD-10-CM_Code` as ICD10, CCSR_Category,CCSR_Category_Description from cdh_reference_data.ccsr_codelist) cr on replace(dx.prydxicd10, ". ", " ")=cr.ICD10            where 1=1             and personid in (select personid from cdh_truveta_exploratory.tog0_truveta_matchcasecontrolIDD)  ") prdx.write.mode( "overwrite ").saveAsTable( "cdh_sandbox.tog0_truveta_prydxccsrIDD_ "+RunID)"," # Get population of all patients with Primary dx of (cap,uti and diabetes) between 2016 -2023 and apply exclusion criteria pqiex by removing patients in pqiex  #primary diagnosis of disease (cap,uti and ud) and the encounter associated with the diagnosis s=spark.sql(" with dx as (select distinct encounterid, dx             from (select c.personid,encounterid,                  case                   when ct.conceptcode in ('J13','J14','J15.211','J15.212','J15.3','J15.4','J15.7','J15.9','J16.0','J16.8','J18.0','J18.1','J18.8','J18.9') then 'cap'                  when ct.conceptcode in ('N28.86','N30.00','N30.01','N30.90','N30.91','N39.0','N10','N12','N15.1','N15.9','N16','N28.84','N28.85') then 'uti'                  when ct.conceptcode in ('E10.649','E10.65','E11.649','E11.65','E13.649','E13.65') then 'udb'                  end as dx                  from cdh_truveta.condition c                  left join cdh_truveta.conditioncodeconceptmap m on c.codeconceptmapid=m.id                  left join cdh_truveta.concept ct on m.codeconceptid=ct.conceptid                  left join cdh_truveta.concept ctc on c.primarydiagnosisconceptid=ctc.conceptid                  where 1=1                  and recordeddatetime between '2016-01-01' and '2023-12-31'                  and ctc.conceptname='Yes' --Primary diagnosis                 ) t              where not exists (select 1 from pqiex                                 where t.personid=pqiex.personid                               ) ), --All patient(IDD vs. No IDD) encountes  penc as (select id,idd          from(select id,idd, row_number() over(partition by id order by id) as rn                from cdh_truveta_exploratory.tog0_truveta_matchcasecontrolIDD                where clr='inp'               ) where  rn=1         )   select id,idd,coalesce(cap,0 ) as cap,coalesce(uti,0) as uti,coalesce(udb,0) as udb from( select * from(select id,idd,dx      from penc       left join dx on penc.id=dx.encounterid    ) p    pivot    (          count(1)          for dx in ('cap','uti','udb')    )                                                                                          ) ").write.mode( "overwrite ").saveAsTable( "cdh_sandbox.tog0_truveta_caputid_ "+RunID)  
# MAGIC ```
# MAGIC
