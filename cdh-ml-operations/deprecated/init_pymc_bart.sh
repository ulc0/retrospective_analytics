#!/bin/bash -xv
cd /dbfs/packages/wheels
pip install --upgrade pip
# Note: PYMC is installed in separate script
# pymc-bart is an add one
pip install scipy==1.10.1
pip install -U pymc_bart-0.5.1-py3-none-any.whl
#pip install --no-deps numpy==1.25.2

