#!/bin/bash -xv
cd /dbfs/packages/wheels
pip install --upgrade pip
###pip install -U numpy==1.21
# CUDA 12 installation
# Note: wheels only available on linux.
####pip install --upgrade "jax[cuda12_pip]" -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html

# CUDA 11 installation
# Note: wheels only available on linux.
#pip install --upgrade "jax[cuda11_pip]" -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
####pip install numpyro[cuda] -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
# will install jacks
#pip install blackjax
###pip install zarr
####pip install mkl
###pip install mkl-service
#pip install libblas=*=*mkl
###pip install -U scipy>=0.14
###pip install numba>=0.57
###pip install numba-scipy
###pip install -U cons==0.4.6
###pip install -U etuples==0.3.9
####pip install -U miniKanren==1.0.3
####pip install -U logical-unification
###pip install -U typing_extensions
# complete=[jax,numba]
###pip install -U pytensor==2.14.2
# Instal graphviz for pymc
pip install --no-deps pygraphviz
pip install  --no-deps arviz 
pip install -U --no-deps pytensor==2.17.3
pip install -U numba==0.57.1
###pip install -U --no-deps pymc-5.7.2-py3-none-any.whl
###pip install -U --no-deps pymc_bart-0.5.1-py3-none-any.whl
pip install -U  --no-deps pymc-5.9.0-py3-none-any.whl
pip install bambi
#pip install --no-deps numpy==1.25.2

