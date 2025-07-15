NO
#!/bin/bash -xv
pip install --upgrade pip
cd /dbfs/packages/wheels
pip install -U --no-deps ecos
pip install -U --no-deps osqp
pip install -U --no-deps scikit-learn==1.2.0
pip install -U  scikit-survival
