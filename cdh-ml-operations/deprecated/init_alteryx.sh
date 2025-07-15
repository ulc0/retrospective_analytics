
#!/bin/bash -xv
pip install --upgrade pip
pip install --no-deps "woodwork[spark]"==0.23.0
#pip install stumpy
#pip install tsfresh
#pip install zict
pip install --no-deps "featuretools[spark,nlp,sklearn]"==1.25.0
