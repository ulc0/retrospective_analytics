# Databricks notebook source
# MAGIC %md
# MAGIC https://www.databricks.com/blog/2023/02/06/getting-started-nlp-using-hugging-face-transformers-pipelines.html

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")

hfpipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="none",framework="pt",device=0) # pass device=0 if using gpu



# COMMAND ----------

ptest="pv: tzm ekg report: heart rate: 64 bpm p - r wave interval: 194 msec qt interval: 380 msec corrected qt interval: 387 msec qrs width: 88 msec p wave frontal axis: 74 ° qrs complex frontal axis: 51 ° t wave frontal axis: 72 ° interpretation: sinus rhythm - negative precordial t-waves ."
results=hfpipe("pv: tzm ekg report: heart rate: 64 bpm p - r wave interval: 194 msec qt interval: 380 msec corrected qt interval: 387 msec qrs width: 88 msec p wave frontal axis: 74 ° qrs complex frontal axis: 51 ° t wave frontal axis: 72 ° interpretation: sinus rhythm - negative precordial t-waves .")


# COMMAND ----------

print(results)

# COMMAND ----------

preds = [
    {
        "entity": pred["entity"],
        "score": round(pred["score"], 4),
        "index": pred["index"],
        "word": pred["word"],
        "start": pred["start"],
        "end": pred["end"],
    }
    for pred in preds
]
print(preds)
