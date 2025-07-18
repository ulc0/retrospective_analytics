# Databricks notebook source
# MAGIC %md
# MAGIC UMLS REST API
# MAGIC
# MAGIC https://documentation.uts.nlm.nih.gov/rest/home.html
# MAGIC
# MAGIC https://documentation.uts.nlm.nih.gov/automating-downloads.html
# MAGIC

# COMMAND ----------

import os, requests, json
CATALOG='edav_dev_cdh_test'
SCHEMA='dev_cdh_ml_test'
DOWNLOAD_URI=f"https://uts-ws.nlm.nih.gov/download?url="

# COMMAND ----------

data_list=spark.read(f"{CATALOG}.{SCHEMA}.umls_releases")
display(data_list)

# COMMAND ----------


dbutils.widgets.dropdown("DATA_PRODUCT","UMLS Metathesaurus Full Subset",keys)
dbutils.widgets.text("API_KEY","789be32d-9376-4611-a0d5-6e5ff6f7955c")


# COMMAND ----------

#Select Data Type, this should be parameterized at some point
# API_KEY should be parameterized
#DATA_PRODUCT='RxNorm Full Monthly Release' #"UMLS Full Release"
DATA_PRODUCT=dbutils.widgets.get("DATA_PRODUCT")
API_KEY="789be32d-9376-4611-a0d5-6e5ff6f7955c"
DATA_CATEGORY=DATA_PRODUCT.lower().split(' ')[0]+'/'  # first word, lower
print(DATA_CATEGORY)

                     #https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_weekly_10052022.zip&apiKey=YOUR_API_KEY"

# COMMAND ----------

# selected the data product
UMLS=dataproducts[DATA_PRODUCT]
#VERSION_URI=f"{DOWNLOAD_URI}{UMLS}&apiKey={API_KEY}"
VERSION_URI=f"{UMLS}&apiKey={API_KEY}"
print(VERSION_URI)

# COMMAND ----------

r = requests.get(VERSION_URI)
latest=r.json()[0]  #latest for now
print(latest)

# COMMAND ----------

LURI=latest['downloadUrl']
print(LURI)

FILENAME=LURI.split('/')[-1]
print(FILENAME)

# COMMAND ----------

APIURI=f"{DOWNLOAD_URI}{LURI}&apiKey={API_KEY}"
print(APIURI)

# COMMAND ----------

r = requests.get(APIURI)


# COMMAND ----------



open(DIRECTORY+FILENAME , 'wb').write(r.content)


# COMMAND ----------

dbutils.fs.ls(DIRECTORY)

# COMMAND ----------

import zipfile
with zipfile.ZipFile(DIRECTORY+FILENAME, 'r') as zip_ref:
    zip_ref.extractall(DIRECTORY+DATA_CATEGORY)

# COMMAND ----------

# MAGIC %md
# MAGIC from **README.txt**
# MAGIC May 2024
# MAGIC
# MAGIC README
# MAGIC
# MAGIC UMLS 2024AA Release
# MAGIC
# MAGIC What's New
# MAGIC -------------------
# MAGIC The 2024AA release of the Unified Medical Language System(R) (UMLS) Knowledge Sources is available for download as of May 6, 2024. 
# MAGIC
# MAGIC The available downloads are:
# MAGIC  - UMLS Metathesaurus Precomputed Subsets (Requires no installation, see below for more information.)
# MAGIC  - MRCONSO.RRF file (Most widely used Metathesaurus file.)
# MAGIC  - Full Release (UMLS Metathesaurus, Semantic Network, Specialist Lexicon and Lexical Tools, database load scripts, and MetamorphoSys for customizing your UMLS subset and browsing the data.)
# MAGIC  - UMLS Metathesaurus History Files (Files that contain historical data from the UMLS Metathesaurus.)
# MAGIC
# MAGIC UMLS Metathesaurus Precomputed Subsets
# MAGIC
# MAGIC In past years, we have required the use of MetamorphoSys to install and customize the UMLS Metathesaurus. Now we are providing two ready-to-use precomputed subsets of the UMLS Metathesaurus that require no installation or customization.
# MAGIC
# MAGIC  - UMLS Metathesaurus Full Subset (complete Metathesaurus data without any customization)
# MAGIC  - UMLS Metathesaurus Level 0 Subset (Includes only level 0 source vocabularies. Level 0 vocabularies do not have any additional restrictions beyond the standard license terms)
# MAGIC
# MAGIC UMLS Metathesaurus History Files
# MAGIC
# MAGIC  - Concept History (MRCONSO_HISTORY.txt) - This file contains all atoms, concepts, and codes that were dropped from the UMLS Metathesaurus MRCONSO.RRF starting with the 2004AA release.
# MAGIC  - Relation History (MRREL_HISTORY.txt) - This file contains all relations dropped from the UMLS Metathesaurus MRREL.RRF file starting with the 2004AA release.
# MAGIC  - Source Vocabulary History (MRSAB_HISTORY.txt) - This file includes a row for every version of every UMLS source vocabulary updated in the UMLS Metathesaurus MRSAB.RRF file starting with the 2004AA release.
# MAGIC These files are available from the UMLS Download page: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html
# MAGIC
# MAGIC Please let us know if you find these files useful with comments to the NLM Help Desk (https://support.nlm.nih.gov/support/create-case/) using the subject line: "UMLS History Files".
# MAGIC
# MAGIC
# MAGIC Metathesaurus
# MAGIC -------------------
# MAGIC The 2024AA Metathesaurus contains approximately 3.38 million concepts and 16.4 million unique concept names from 187 source vocabularies.
# MAGIC
# MAGIC Two new translations:
# MAGIC  - MDREST (Estoninan Edition of the Medical Dictionary for Regulatory Activities Terminology (MedDRA).
# MAGIC  - MDRFIN (Finnish Edition of the Medical Dictionary for Regulatory Activities Terminology (MedDRA).
# MAGIC
# MAGIC 33 English sources and 46 translation sources were updated.  These include MeSHR, MedDRA, RxNorm, and SNOMED CTR (English and Spanish).  For detailed information on changes in this version of the Metathesaurus, see the Updated Sources (Expanded) section.  Additional release statistics may be found in the Statistics section.
# MAGIC
# MAGIC SPECIALIST Lexicon and Lexical Tools
# MAGIC -------------------
# MAGIC  - The release includes the updated SPECIALIST Lexicon (2024 Release).
# MAGIC  - The release includes the updated Lexical Tools (2024 Release) which integrate data from the SPECIALIST Lexicon, 2024 Release. The Lexical Tools include the Full and Lite versions of lvg.2024.
# MAGIC The Metathesaurus index files were processed using the updated lvg files.
# MAGIC
# MAGIC MetamorphoSys
# MAGIC -------------------
# MAGIC  - The full release requires 35.1 GB of disk space.
# MAGIC
# MAGIC Reported bugs may be viewed on the Release Notes and Bugs Web page. 
# MAGIC
# MAGIC MetamorphoSys can generate custom load scripts for MySQL, Oracle, or Microsoft Access when creating a Metathesaurus subset or installing the Semantic Network. Instructions are available on the UMLS Load Scripts homepage. 
# MAGIC
# MAGIC Release Information
# MAGIC -------------------
# MAGIC To access the UMLS Release files, you must have an active UMLS MetathesaurusR License and a valid UTS account.  You will be prompted to authenticate with an identity provider with the UTS when downloading the files. 
# MAGIC
# MAGIC UMLS Learning Resources
# MAGIC -------------------
# MAGIC 2024AA Source Release Documentation Web pages are available.  
# MAGIC
# MAGIC Additional information regarding the UMLS is available on the UMLS homepage. New users are encouraged to take the UMLS Basics Tutorial and to explore other training materials.
# MAGIC
# MAGIC UMLS Terminology Services (UTS)
# MAGIC -------------------
# MAGIC The UTS Metathesaurus Browser and the UTS REST API include the updated release. 
# MAGIC
# MAGIC --
# MAGIC
# MAGIC New Terminology Release API Endpoint
# MAGIC
# MAGIC This endpoint provides an updated list of releases for RxNorm, SNOMED CT, and UMLS. You can use this endpoint to completely automate the retrieval of the latest release files without having to manually download the files. For example, if you load RxNorm data weekly or monthly, you can monitor our release API and automatically download the data immediately when it becomes available.
# MAGIC  
# MAGIC Documentation:
# MAGIC https://documentation.uts.nlm.nih.gov/automating-downloads.html
# MAGIC  
# MAGIC Examples:
# MAGIC Request all RxNorm Full Monthly Releases:
# MAGIC https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-full-monthly-release
# MAGIC
# MAGIC Request the current RxNorm Full Monthly Release:
# MAGIC https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-full-monthly-release&current=true
# MAGIC {
# MAGIC "fileName": "RxNorm_full_01022024.zip",
# MAGIC "releaseVersion": "2024-01-02",
# MAGIC "releaseDate": "2024-01-02",
# MAGIC "downloadUrl": https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_01022024.zip,
# MAGIC "releaseType": "RxNorm Full Monthly Release",
# MAGIC "product": "RxNorm",
# MAGIC "current": true
# MAGIC }
# MAGIC
# MAGIC Download the current RxNorm Full Monthly Release:
# MAGIC curl "https://uts-
# MAGIC ws.nlm.nih.gov/download?url=https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_01022024.
# MAGIC zip&apiKey=YOUR_API_KEY" -o RxNorm_full_01022024.zip
# MAGIC  
# MAGIC
# MAGIC UMLS User Contributions
# MAGIC -------------------
# MAGIC UMLS users have extended the functionality of the UMLS in a variety of ways by developing APIs, automation scripts, and natural language processing tools. You can find a list of these on the UMLS Community web page: https://www.nlm.nih.gov/research/umls/implementation_resources/community/index.html. 
# MAGIC
# MAGIC Want to add your tool? Send a request to the NLM Help Desk (https://support.nlm.nih.gov/support/create-case/) with the subject line: "UMLS Community" 
# MAGIC
# MAGIC Be sure to include a link to your source code so that other UMLS users can adapt your tool. We are especially interested in:
# MAGIC  - Database load scripts 
# MAGIC  - Transformation scripts that convert UMLS data into other formats (for example, RDF or JSON)
# MAGIC  - Scripts that automate any aspect of UMLS installation
# MAGIC  - Applications that leverage UMLS in the processing of text
# MAGIC
# MAGIC We value your feedback! For more information about improvements we have made to the UMLS based on user feedback, as well as information about UMLS use in general, see our UMLS User Feedback Page: https://www.nlm.nih.gov/research/umls/implementation_resources/community/user_feedback.html
# MAGIC
# MAGIC
# MAGIC
# MAGIC -------------------
# MAGIC For information on the Unified Medical Language System (UMLS), consult the UMLS homepage at: https://www.nlm.nih.gov/research/umls.
# MAGIC
# MAGIC The latest Release Notes list all known issues, including bugs and fixes, and are available at: https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/notes.html.
# MAGIC
# MAGIC UMLS data files and MetamorphoSys are available by download from the UMLS Web site at: https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html. Users must have an active UMLS Terminology Services (UTS) account to download the files. All files must be downloaded into the same directory.  MetamorphoSys must be unzipped before it can be used.
# MAGIC
# MAGIC
# MAGIC
# MAGIC HARDWARE AND SOFTWARE REQUIREMENTS
# MAGIC ------------------------------------------------------------------
# MAGIC
# MAGIC Supported operating systems:
# MAGIC
# MAGIC Windows
# MAGIC Linux
# MAGIC macOS
# MAGIC
# MAGIC Hardware Requirements
# MAGIC
# MAGIC   - A MINIMUM 40 GB of free hard disk space.  
# MAGIC   - A MINIMUM of 2 GB of RAM, preferably more. Smaller memory size will cause virtual memory paging with exponentially increased processing time.
# MAGIC   - A CPU speed of at least 2 GHz for reasonable installation times.
# MAGIC  
# MAGIC
