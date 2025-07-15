# Databricks notebook source
#%pip upgrade pip
%pip install pipdeptree

# COMMAND ----------

# MAGIC %md
# MAGIC list --format json

# COMMAND ----------

import subprocess

# COMMAND ----------



p = subprocess.Popen('pip list --format json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
lines=[]
for line in p.stdout.readlines():
    lines.append(line)
retval = p.wait()
print(lines)

# COMMAND ----------

import json
#print(lines[0]) #json b-string terminated with \n
pipdict=json.loads(lines[0])
print(pipdict)

# COMMAND ----------

# MAGIC %sh
# MAGIC pipdeptree --json-tree

# COMMAND ----------

p = subprocess.Popen(f'pipdeptree --json-tree', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
lines=[]
for line in p.stdout.readlines():
    lines.append(line.decode("utf-8").rstrip('\n'))
retval = p.wait()
#piptree=json.loads(lines)
#print(piptree)


# COMMAND ----------

sjson=(''.join(lines))
sdict=json.loads(sjson)
print(sdict)

# COMMAND ----------

ddict={}
for pdict in pipdict:
    p = subprocess.Popen(f'pipdeptree --packages {pdict["name"]} --json', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines=[]
    for line in p.stdout.readlines():
        lines.append(line)
    retval = p.wait()
    #d=lines[0]
    print(pdict)
    print(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC install johnnydep pipdeptree pydeps

# COMMAND ----------

# MAGIC %md
# MAGIC We're going to use johnnydep to make dot graphs, and create a supergraph with all those graphs as subgraphs

# COMMAND ----------

# MAGIC %md
# MAGIC !johnnydep --help

# COMMAND ----------

# MAGIC %md
# MAGIC !johnnydep pymc --output-format json -f name 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC import pathlib
# MAGIC rfile='requirements-12-2-ml.txt'
# MAGIC import pkg_resources
# MAGIC import setuptools
# MAGIC
# MAGIC with pathlib.Path(rfile).open() as requirements_txt:
# MAGIC     install_requires = [
# MAGIC         str(requirement)
# MAGIC         for requirement
# MAGIC         in pkg_resources.parse_requirements(requirements_txt)
# MAGIC     ]
# MAGIC print(install_requires)
# MAGIC #setuptools.setup(
# MAGIC #    install_requires=install_requires,
# MAGIC #)

# COMMAND ----------

from pip._vendor import import_lib.resources
for package_name,package_version in packagedict.items():
#package_name="pandas"
    package = pkg_resources.working_set.by_key[package_name]
    print(package.requires())
    for dependency in package.requires():
        print(str(dependency))




# COMMAND ----------

# MAGIC %pip show numpy > numpy.txt

# COMMAND ----------

!pipdeptree

# COMMAND ----------

!pydeps numpy --show-deps --max-bacon 7 --pylib -x os re types _* enum

# COMMAND ----------

!pipdeptree --reverse --packages pandas
