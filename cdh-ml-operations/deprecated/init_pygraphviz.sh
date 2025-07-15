DO NOT USE, use networkx
#!/bin/bash -xv
cd /dbfs/packages
#sudo rm /var/lib/apt/lists/lock
#sudo apt-get update
# https://kb.databricks.com/libraries/install-pygraphviz.html
#sudo apt-get install -y python3-dev graphviz libgraphviz-dev pkg-config 
#pip install pygraphviz
pip install networkx[default,extra]