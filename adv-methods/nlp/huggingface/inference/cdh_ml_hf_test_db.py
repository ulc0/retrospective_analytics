# Databricks notebook source
# MAGIC %md
# MAGIC https://www.databricks.com/blog/2023/02/06/getting-started-nlp-using-hugging-face-transformers-pipelines.html

# COMMAND ----------

# MAGIC %md
# MAGIC huggingface_hub[tensorflow,torch]==0.22.2

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

import mlflow, torch
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

class ClassificationPipelineModel(mlflow.pyfunc.PythonModel):
  def load_context(self, context):
    device = 0 if torch.cuda.is_available() else -1
    print(device)
    self.pipeline = pipeline("ner", context.artifacts["pipeline"], device=device)
    
  def predict(self, context, model_input): 
    texts = model_input.iloc[:,0].to_list() # get the first column
    pipe = self.pipeline(texts, truncation=True, batch_size=8)
    summaries = [summary['summary_text'] for summary in pipe]
    return pd.Series(summaries)

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")

# aggregation_strategy="simple",
hfpipe = pipeline("ner", model=model, tokenizer=tokenizer,framework="pt",device=0) # pass device=0 if using gpu



# COMMAND ----------

ptest=("pv: tzm ekg report: heart rate: 64 bpm p - r wave interval: 194 msec qt interval: 380 msec corrected qt interval: 387 msec qrs width: 88 msec p wave frontal axis: 74 ° qrs complex frontal axis: 51 ° t wave frontal axis: 72 ° interpretation: sinus rhythm - negative precordial t-waves .")
results=hfpipe(ptest)


# COMMAND ----------

display(results)
