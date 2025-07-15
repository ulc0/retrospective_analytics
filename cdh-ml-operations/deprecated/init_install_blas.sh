#!/bin/bash -xv
cd /dbfs/packages
sudo rm /var/lib/apt/lists/lock
sudo apt-get update
sudo apt-get install libblas-dev liblapack-dev 