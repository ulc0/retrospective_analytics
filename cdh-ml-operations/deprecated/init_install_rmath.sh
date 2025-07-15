#!/bin/bash -xv
cd /dbfs/packages
git clone git@github.com:statslabs/rmath.git
#
cd rmath
mkdir build && cd build
cmake ..
#
make
make install