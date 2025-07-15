# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


# Define source and target table names
source_table = "edav_project_audit_logs.cdh.audit"
target_table = "edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver"

# Fetch the latest timestamp from the target table
silver_df = spark.sql(f"SELECT MAX(event_date) AS latest_timestamp FROM {target_table}")
silver_latest_timestamp = silver_df.collect()[0]["latest_timestamp"]

print(f"Latest Timestamp from {target_table}: {silver_latest_timestamp}")

# Fetch the latest timestamp from the source table
audit_df = spark.sql(f"SELECT MAX(event_date) AS latest_timestamp FROM {source_table}")
audit_latest_timestamp = audit_df.collect()[0]["latest_timestamp"]

print(f"Latest Timestamp from {source_table}: {audit_latest_timestamp}")

# Convert timestamps to date format (YYYY-MM-DD) for comparison
if silver_latest_timestamp and audit_latest_timestamp:
    silver_date = silver_latest_timestamp.strftime("%Y-%m-%d")
    audit_date = audit_latest_timestamp.strftime("%Y-%m-%d")

    # Exit notebook if the latest timestamp is from the same day
    if silver_date == audit_date:
        print("Latest timestamp is from the same day. Exiting the notebook.")
        dbutils.notebook.exit("No new data to process.")

# If timestamps are different, proceed with incremental data extraction
df = spark.sql(f"""
    select event_date, 
            request_params.commandText, 
            user_identity.email, 
            request_params.clusterId, 
            request_params.notebookId,
            request_params.executionTime,
            request_params.commandId
    FROM {source_table}
    WHERE event_date > '{silver_latest_timestamp}'
    AND action_name = 'runCommand'
""")

# Append new records if available
if df.count() > 0:

    print(" Continue Process")
else:
    print("No new data to load.")


# COMMAND ----------

# MAGIC %md
# MAGIC cdh_Premier_v2

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import lit

# Define the regex pattern to extract table names
pattern = r'(\bcdh_premier_v2\.[a-zA-Z_][\w]*)'

# Apply regex extraction to the "commandText" column
df_premier = df.withColumn("extracted_tables", regexp_extract(col("commandText"), pattern, 1))

# Show extracted table names
# df.select("commandText", "extracted_tables").show(truncate=False)


# Add the pattern as a new column
df_premier = df_premier.withColumn("regex_pattern", lit(pattern))

# Display the DataFrame with the added pattern column

display(df_premier)

# COMMAND ----------

# MAGIC %md
# MAGIC cdh_truveta

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import lit

# Define the regex pattern to extract table names
pattern = r'(\bcdh_truveta\.[a-zA-Z_][\w]*)'

# Apply regex extraction to the "commandText" column
df_truveta = df.withColumn("extracted_tables", regexp_extract(col("commandText"), pattern, 1))

# Show extracted table names
# df.select("commandText", "extracted_tables").show(truncate=False)


# Add the pattern as a new column
df_truveta = df_truveta.withColumn("regex_pattern", lit(pattern))

# Display the DataFrame with the added pattern column

display(df_truveta)

# COMMAND ----------

# MAGIC %md
# MAGIC ALL Others

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import lit

# Define the regex pattern to extract table names
pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)'

# Apply regex extraction to the "commandText" column
df_others = df.withColumn("extracted_tables", regexp_extract(col("commandText"), pattern, 1))

# Show extracted table names
# df.select("commandText", "extracted_tables").show(truncate=False)


# Add the pattern as a new column
df_others = df_others.withColumn("regex_pattern", lit(pattern))

# Display the DataFrame with the added pattern column

display(df_others)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter DataFrame to keep only rows where 'extracted_tables' is not an empty string
df_others = df_others.filter(col("extracted_tables") != "")

# Display the filtered DataFrame
# display(df_Others)

from pyspark.sql.functions import split, col

# Split the 'extracted_tables' column into two parts using the period as a delimiter
df_others= df_others.withColumn("schema_name", split(col("extracted_tables"), "\.").getItem(1)) \
       .withColumn("table_name", split(col("extracted_tables"), "\.").getItem(2))

# Display the results
# df.select("extracted_tables", "schema_name", "table_name").show(truncate=False)

display(df_others)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter DataFrame to keep only rows where 'extracted_tables' is not an empty string
df_premier = df_premier.filter(col("extracted_tables") != "")

# Display the filtered DataFrame
# display(df_premier)

from pyspark.sql.functions import split, col

# Split the 'extracted_tables' column into two parts using the period as a delimiter
df_split_premier = df_premier.withColumn("schema_name", split(col("extracted_tables"), "\.").getItem(0)) \
       .withColumn("table_name", split(col("extracted_tables"), "\.").getItem(1))

# Display the results
# df.select("extracted_tables", "schema_name", "table_name").show(truncate=False)

display(df_split_premier)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter DataFrame to keep only rows where 'extracted_tables' is not an empty string
df_truveta = df_truveta.filter(col("extracted_tables") != "")

# Display the filtered DataFrame
# display(df_truveta)

from pyspark.sql.functions import split, col

# Split the 'extracted_tables' column into two parts using the period as a delimiter
df_split_truveta= df_truveta.withColumn("schema_name", split(col("extracted_tables"), "\.").getItem(0)) \
       .withColumn("table_name", split(col("extracted_tables"), "\.").getItem(1))

# Display the results
# df.select("extracted_tables", "schema_name", "table_name").show(truncate=False)

display(df_split_truveta)

# COMMAND ----------

# Assuming df1 and df2 are your two DataFrames
df_union_all = df_split_premier.unionAll(df_split_truveta).unionAll(df_others)

# Show the result of the union
display(df_union_all)

# COMMAND ----------

# %sql
#  show create table edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver

# COMMAND ----------

# %sql
# drop table if exists edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver;
# CREATE TABLE edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver (
#   event_date DATE,
#   commandtext STRING,
#   email STRING,
#   clusterid STRING,
#   notebookid STRING,
#   schema_name STRING,
#   table_name STRING,
#   execution_time FLOAT,
#   commandid STRING,
#   sessionid STRING,
#   regex_pattern STRING)
# USING delta
# TBLPROPERTIES (
#   'delta.checkpoint.writeStatsAsJson' = 'false',
#   'delta.checkpoint.writeStatsAsStruct' = 'true',
#   'delta.minReaderVersion' = '1',
#   'delta.minWriterVersion' = '2')

# COMMAND ----------

# Register DataFrame as a temporary view
df_union_all.createOrReplaceTempView("temp_view")

# Use SQL to insert data
spark.sql("""
                            insert into edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver
    (
			event_date,
			commandText, 
			email,
			clusterid, 
			notebookid,
			schema_name,
			table_name,
			execution_time,
            commandId,
            regex_pattern
   
    )
    SELECT 	event_date,
			commandText, 
			email,
			clusterid, 
			notebookid,
			schema_name,
			table_name,
			executiontime,
            commandId,
            regex_pattern
 	FROM temp_view
""")


# COMMAND ----------

# Use SQL to insert data
# spark.sql("""
# INSERT INTO edav_prd_cdh.cdh_engineering_etl.analytics_cluster_tables
# (
#   Schema_name, 
#   Table_Name
# )
# SELECT Schema_Name, Table_name
# FROM edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver
# GROUP BY Schema_name, Table_name
# """)

# COMMAND ----------

# Use SQL to insert data

spark.sql("""
          MERGE INTO edav_prd_cdh.cdh_engineering_etl.analytics_cluster_tables AS t
USING (SELECT schema_name, table_name
FROM edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver
where table_name is not null
GROUP BY schema_name, table_name) AS s
ON t.schema_name = s.schema_name 
    AND T.table_name = s.table_name
WHEN NOT MATCHED THEN 
INSERT (schema_name, table_name) VALUES (s.schema_name, s.table_name)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Pattern3 