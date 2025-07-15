# Databricks notebook source
import requests

API_URL = "https://api-inference.huggingface.co/models/bigcode/starpii"
headers = {"Authorization": "Bearer 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"}

def query(payload):
	response = requests.post(API_URL, headers=headers, json=payload)
	return response.json()
	
output = query({
	"inputs": "My name is Sarah Jessica Parker but you can call me Jessica",
})

# COMMAND ----------

print(output)
