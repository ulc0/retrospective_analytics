#!/bin/bash -xv
cd /dbfs/packages/wheels
pip install -U --no-deps spark_nlp-4.3.2-py2.py3-none-any.whl
pip install -U --no-deps Bio_Epidemiology_NER-0.1.3-py3-none-any.whl
pip install -U --no-deps nlu-4.2.2-py3-none-any.whl