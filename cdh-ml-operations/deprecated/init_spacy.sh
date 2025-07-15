#!/bin/bash -xv
pip install --upgrade pip
#pip install nmslib==1.7.3.6
#pip install scipy==1.10.1
cd /dbfs/packages/wheels
#pip install -U --no-deps spacy_lookups_data #-1.0.3-py2.py3-none-any.whl
#pip install -U --no-deps spacy_lookups_data-1.0.5-py2.py3-none-any.whl
#pip install -U --no-deps spacy_transfomers-1.2.5-py2.py3-none-any.whl
#pip install -U --no-deps conllu-4.5.3-py2.py3-none-any
#pip install -U --no-deps scikit_learn-1.3.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
#pip install -U --no-deps spacy-3.4.4-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
#pip install -U --no-deps spacy-3.6.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
#pip install -U --no-deps scispacy-0.5.3-py3-none-any.whl
#pip install -U --no-deps spacy_llm-0.4.3-py2.py3-none-any.whl
#pip install -U --no-deps spacy_llm-0.5.1-py2.py3-none-any.whl
#pip install -U --no-deps Bio_Epidemiology_NER-0.1.3-py3-none-any.whl
cd /dbfs/packages/sdist
cat scispacy_models.sh
./scispacy_models.sh