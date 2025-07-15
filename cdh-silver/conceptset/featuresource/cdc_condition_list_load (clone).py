# Databricks notebook source
# ------------------------------------------------------------------------------------------------------------ #
# Name    : cdh_reference_data.cdc_condition_list                                                              #
# Source  : Curated/reference_data/original_files                                                              #
# -------------------------------------------------------------------------------------------------------------#
# -------------------------------------------------------------------------------------------------------------#
#  History                                                                                                     
#  11/15/21            Created delta load for CDC Condition List         Dillard George (qoh3@cdc.gov)                                                                       
# -------------------------------------------------------------------------------------------------------------#

# COMMAND ----------

# MAGIC %run /CDH/Developers/Common/cdhFunctions
# MAGIC

# COMMAND ----------

# set ADLS gen secure connection variables
adlsAccountName 	= "davsynapseanalyticsdev"
adlsContainerName 	= "cdh"
adlsFolderName 		= "curated"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# call secureConnect_adls common function 
cdhSecureConnectSpark_adls(storageAccountName=adlsAccountName,
                   containerName=adlsContainerName,
                   datahubZoneName=adlsFolderName
                     )

# COMMAND ----------

# Database and table format Variables 
dataSource		= "cdc_condition_list"
deltaDatabase	= "cdh_reference_data"
tableFormat		= "delta"
sourceFormat    = "csv"
deltaFolderLocation 	= source + '/reference_data/delta/' + dataSource
sourceFolderLocation   = source + '/reference_data/source' + dataSource

# print(deltaFolderLocation)
# print(parquetFolderLocation)
# print(deltaDatabase)


# COMMAND ----------

# Create delta tables 
# Source : PPC tables 
# Note : Fixed table list . Must change based onthe new schema objects.

# Dynamic list of tables 
tables = cdhGetTableList(sourceFolderLocation)

counter = 0
for table in tables:
      deltaFileLocation	= deltaFolderLocation + '/' + table
      sourceFileLocation = sourceFolderLocation +  '/' + table
      # print(deltaFileLocation)
      # print(parquetFileLocation)
      
      # Call to common function to create temp parquet for overwrite full delta
      counter = counter + 1
      cdhCreateTableDeltaFromTempParquet (deltaDatabase, table, tableFormat, parquetFileLocation, deltaFileLocation ,counter  )
      # print(table)
      

# COMMAND ----------

# ---- end ------
