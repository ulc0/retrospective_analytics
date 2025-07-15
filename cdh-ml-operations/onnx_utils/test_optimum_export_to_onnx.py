# Databricks notebook source
# MAGIC %md
# MAGIC [Huggingface to ONNX](https://github.com/huggingface/optimum)

# COMMAND ----------

# MAGIC %pip install pip optimum[exporters,onnxruntime]

# COMMAND ----------

# MAGIC %sh
# MAGIC optimum-cli export onnx -m deepset/roberta-base-squad2 --optimize O2 roberta_base_qa_onnx

# COMMAND ----------

# MAGIC %sh
# MAGIC optimum-cli onnxruntime quantize \
# MAGIC   --avx512 \
# MAGIC   --onnx_model roberta_base_qa_onnx \
# MAGIC   -o quantized_roberta_base_qa_onnx

# COMMAND ----------

- from transformers import AutoModelForQuestionAnswering
+ from optimum.onnxruntime import ORTModelForQuestionAnswering
  from transformers import AutoTokenizer, pipeline

  model_id = "deepset/roberta-base-squad2"
  tokenizer = AutoTokenizer.from_pretrained(model_id)
- model = AutoModelForQuestionAnswering.from_pretrained(model_id)
+ model = ORTModelForQuestionAnswering.from_pretrained("roberta_base_qa_onnx")
  qa_pipe = pipeline("question-answering", model=model, tokenizer=tokenizer)
  question = "What's Optimum?"
  context = "Optimum is an awesome library everyone should use!"
  results = qa_pipe(question=question, context=context)
