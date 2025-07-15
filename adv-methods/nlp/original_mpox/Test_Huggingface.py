# Databricks notebook source
#%pip install "tensorflow>=2.0.0"

# COMMAND ----------

#%pip install --upgrade tensorflow-hub

# COMMAND ----------

#%pip install transformers

# COMMAND ----------

#%pip install torch 

# COMMAND ----------

#%pip install torchvision

# COMMAND ----------

#%pip install transformers datasets

# COMMAND ----------

# MAGIC %md # Testing Pretrained models

# COMMAND ----------

# MAGIC %md Testing the following task
# MAGIC
# MAGIC 1. Zero shot classification
# MAGIC 2. Name entity recognition
# MAGIC 3. Sentiment analysis
# MAGIC 4. Text classification

# COMMAND ----------

# sample notes

note1 = ["Doubt monkey pox, or zoster.|Hope is not mrsa|Cover with keflex and rto 3-4 d if not responding|9/16/22  Small infection is gone.  Lump itself is not tender now, but same size.  We will refer to surgery for their opinion"]

note2 = ["""RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs; CARDIOVASCULAR: normal rate; rhythm is regular; no systolic murmur; GASTROINTESTINAL: nontender; normal bowel sounds; LYMPHATICS: no adenopathy in cervical, supraclavicular, axillary, or inguinal regions; BREAST/INTEGUMENT: no rashes or lesions; MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone; NEUROLOGIC: appropriate for age; Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius"""]

sequences = note1 + note2

# COMMAND ----------

# MAGIC %md ## Clinical bert 

# COMMAND ----------

# MAGIC %md
# MAGIC cleaned = (
# MAGIC     pxResObs.select("patientuid","note")
# MAGIC     .withColumn("clean_note_RTF", 
# MAGIC                 regexp_replace(F.col("note"), r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","")
# MAGIC                 )
# MAGIC     .withColumn("clean_note_XLM", 
# MAGIC                 regexp_replace(F.col("clean_note_RTF"), r"<[^>]*>|&[^;]+;", "")
# MAGIC                 )
# MAGIC     )
# MAGIC     
# MAGIC     #.where("patientuid in ('231b73b2-bd61-4a7e-ba78-cb28dae7c8da', '75e94e91-7555-4838-aad0-ab8ff7390e9a', '79a58e93-dc05-4372-8e2e-b17276dc4a77')")
# MAGIC     
# MAGIC
# MAGIC display(cleaned)
# MAGIC     
# MAGIC

# COMMAND ----------

from transformers import AutoModel, AutoTokenizer

checkpoint = "emilyalsentzer/Bio_ClinicalBERT"
tokenizer = AutoTokenizer.from_pretrained(checkpoint)

model = AutoModel.from_pretrained(checkpoint)

model_inputs = tokenizer(sequences)

tokens = tokenizer(sequences, padding=True, truncation=True, return_tensors="pt")
output = model(**tokens)

tokens

# COMMAND ----------

#print(tokenizer.decode(model_inputs["input_ids"]))
print(tokenizer.decode(ids))

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification")

#classifier(
#    note1,
#    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer)
concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer, grouped_entities = True)

entities = concept_extraction(note1)

entities

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer)
concept_extraction = pipeline("ner", model=model_name, tokenizer=tokenizer, grouped_entities = True)

entities = concept_extraction(note2)

entities

# COMMAND ----------

# MAGIC %md ### Testing for alternative zero shot (facebook/bart-large-mnli)

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification")

classifier(
    note1,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

classifier(
    note2,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

# sentiment analysis

model_name = "emilyalsentzer/Bio_ClinicalBERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note1,    
)

# COMMAND ----------

model_name = "emilyalsentzer/Bio_ClinicalBERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note2,    
)

# COMMAND ----------

classifier = pipeline("text-classification", model = "emilyalsentzer/Bio_ClinicalBERT")

classifier(
    note1,    
)

# COMMAND ----------

classifier(
    note2,    
)

# COMMAND ----------

# MAGIC %md ## GatorTron-0G

# COMMAND ----------

from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig

MODEL_NAME = 'AshtonIsNotHere/GatorTron-OG'
num_labels = 2

config = AutoConfig.from_pretrained(MODEL_NAME, num_labels=num_labels)
model  =  AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, config=config)
tokenizer  =  AutoTokenizer.from_pretrained(MODEL_NAME)




# COMMAND ----------

featrure_extraction = pipeline("ner", model=model, tokenizer = tokenizer, grouped_entities = True)

classifier(
    note1,    
)

# COMMAND ----------

featrure_extraction = pipeline("ner", model=model, tokenizer = tokenizer)

classifier(
    note2,    
)

# COMMAND ----------

model_name = "AshtonIsNotHere/GatorTron-OG"
classifier = pipeline("ner", model=model_name, tokenizer=tokenizer)


classifier(
    note2,    
)

# COMMAND ----------

#zero shot

#classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer)
classifier = pipeline("zero-shot-classification",  model=model, tokenizer = tokenizer)

classifier(
    note1,
    candidate_labels=["sexual transmited disease", "pox like virus", "skin"],
)

# COMMAND ----------

classifier = pipeline("sentiment-analysis",  model=model, tokenizer = tokenizer)

classifier(
    note1,    
)

# COMMAND ----------

# MAGIC %md ## PHS-BERT

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("publichealthsurveillance/PHS-BERT")
model = AutoModel.from_pretrained("publichealthsurveillance/PHS-BERT")

# COMMAND ----------

model_name = "publichealthsurveillance/PHS-BERT"
classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer)


classifier(
    note1,    
)

# COMMAND ----------

classifier(
    note2,    
)

# COMMAND ----------

# MAGIC %md ## DIstilled BERT
