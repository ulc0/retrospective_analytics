# Databricks notebook source
# dictionary to control hcup, currently maintained in code, should be user maintained
# base_url=https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2023-1.zip
#C:\Users\ulc0\Downloads\DXCCSR_v2022-1.zip
# key is both address and .csv file name
base_url="https://hcup-us.ahrq.gov/toolssoftware/ccsr/"

# COMMAND ----------


dbutils.widgets.text("FEATURE","ccsr")
dbutils.widgets.text("VERSION","2022_1")
FEATURE=dbutils.widgets.get("FEATURE")
VERSION=dbutils.widgets.get("VERSION")


# COMMAND ----------

hcup = {
    "ccsr": {
        "2023_1": {
            "filename": "DXCCSR_v2023-1",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2023-1.zip",
        },
        "2022_1": {
            "filename": "DXCCSR_v2022-1",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2022-1.zip",
        },
        "2021_1": {
            "filename": "DXCCSR_v2021-1",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2021-1.zip",
        },
        "2021_2": {
            "filename": "DXCCSR_v2021",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2021-2.zip",
        },
        "2020_3": {
            "filename": "DXCCSR_v2020",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/v2020-3.zip",
        },
        "2020_2": {
            "filename": "DXCCSR_v2020",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/v2020_2.zip",
        },
        "2020_1": {
            "filename": "DXCCSR_v2020",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR2020_1.zip",
        },
        "2019_1": {
            "filename": "DXCCSRv2019",
            "uri": "https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR2019_1.zip",
        },
    },
}

# COMMAND ----------


from zipfile import ZipFile
#archive = zipfile.ZipFile('images.zip', 'r')
#imgdata = archive.read('img_01.png')

# COMMAND ----------



# COMMAND ----------

releases=hcup[FEATURE]
release=releases[VERSION]
cpath=f"/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/hcup/"
print(cpath)

# COMMAND ----------

import requests
fn=release["filename"]
uri=release["uri"]
response=requests.get(uri)
print(response.status_code)
zipfn=f"{cpath}{fn}.zip"
open(zipfn , 'wb').write(response.content)



# COMMAND ----------

with ZipFile(zipfn, 'r') as archive:
    archive.printdir()
   # Extract all the contents of zip file in different directory
    archive.extractall(f"{cpath}{FEATURE}/{VERSION}/")
