# Databricks notebook source
# MAGIC %md
# MAGIC ### https://learn.microsoft.com/en-us/azure/databricks/archive/dev-tools/python-api

# COMMAND ----------

# MAGIC %pip install databricks-cli

# COMMAND ----------

import base64
import time
import argparse
import base64
import json, os
import requests

from databricks_cli.sdk.api_client import ApiClient

from databricks_cli.jobs.api import JobsApi

from databricks_cli.runs.api import RunsApi

# COMMAND ----------

api_client = ApiClient(
  host  = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)
