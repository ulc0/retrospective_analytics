#!/bin/bash -xv
cd /dbfs/packages/wheels
#'torchmetrics>=0.10.0,<0.12'
pip install -U --no-deps torchmetrics-0.11.4-py3-none-any.whl
#'torch_optimizer>=0.3.0,<0.4',
pip install -U --no-deps torch_optimizer-0.3.0-py3-none-any.whl
#'coolname>=1.1.0,<3',
pip install -U --no-deps coolname-2.2.0-py3-none-any.whl
#'tabulate==0.9.0',  # for auto-generating tables overwrites 13.2
pip install -U --no-deps tabulate-0.9.0-py3-none-any.whl
#'py-cpuinfo>=8.0.0,<10',
pip install -U --no-deps py-cpuinfo-9.0.0-py3-none-any.whl
#'mosaicml-cli>=0.4.0,<0.5',
pip install -U --no-deps mosaicml-cli-0.4.12-py3-none-any.whl
# base composer
pip install -U --no-deps mosaicml-0.1.0-py3-none-any.whl
