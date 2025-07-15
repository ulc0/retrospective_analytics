#Databricks System Tables 
## Facts Common to all System Tables (facts of interest **bolded**)
  - **event_id** string: unique key for the transaction event that may span many records
  - accountid string: *constant* (EDAV)
  - workspaceid string: *constant* (Production)
  - **event_time** timestamp: time in UTC
  - event_date date: event_time cast as DATE 

#[Databricks Audit Logs](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs)

## Scope
* Audit records for CDH at the "service" API transaction level
* Product Analytics requires services:
  - "SQL": service_name='databrickssql'OR
  - "JOB": serviceName="jobs" and action_name="runCommand"
  - "NOTEBOOK": serviceName="notebook" and action_name="runCommand"
## Granularity
* the API call level, which is commandid/commandtext for SQL, JOB, and NOTEBOOK services
* Drill-down hierarchy
  * accountid
    * workspaceid
      * session_id
        * jobId/jobRunId
          * notebookid (notebook/job)
            * commandid 
## Facts (facts of interest **bolded**)
*  Command Facts 
  - version string: log schema version (2.0)
  - source_ip_address string: client ip address (if applicable)
  - user_agent string: API user agent
  - **session_id** string: session_id of the command
  - **user_identity** struct: {"email": "user@domain.com","subjectName": null}
  - **service_name** string: the name of the API service
  - **action_name** string: the action to be performed
  - **[identity_metadata](https://learn.microsoft.com/en-us/azure/databricks/compute/group-access#audit-group)** struct: {run_by: "example@email.com" run_as: "example@email.com"}
  - **request_parameters** map : the API call payload/parameters, differs by **service_name**

### audit.request_parms for SQL, JOB, NOTEBOOK
serviceName="databrickssql"
* warehouseId is a clusterid
```python
warehouseId: "8a06c2c09007645d"
commandParameters: "[]"
commandId: "01f00af8-8836-1e1d-8462-734c1ce092a4"
commandText: "update \n                    edav_prd_cdh.cdh_engineering.etl_workflow_service_scheduled\n                    set processed_id = '555072ee9319490a9908252bec3375f9', last_updated_date_utc = now()\n                    where request_id in (select request_id \n                                         from edav_prd_cdh.cdh_engineering.etl_workflow_service_scheduled \n                                         where from_utc_timestamp(now(), 'US/Eastern') > prevent_run_window_end_time_et\n                                         and processed_id is null\n                                         );\n                     "
```

serviceName="jobs" and action_name="runCommand"
```python
notebookId: "1931918317680194"
clusterId: "0109-184947-l0ka6b1y"
executionTime: "0.093"
status: "finished"
**jobId**: "29685044068930"**
commandLanguage: "python"
commandId: "7179763816500554573"
**runId**: "128631309079939"**
commandText: "#concept_set/concept_set_table.py\ndbutils.widgets.text(\"SRC_CATALOG\",defaultValue=\"edav_prd_cdh\")\nSRC_CATALOG=dbutils.widgets.get(\"SRC_CATALOG\")\ndbutils.widgets.text('SRC_SCHEMA',defaultValue='cdh_truveta_lava_dev')\nSRC_SCHEMA=dbutils.widgets.get('SRC_SCHEMA')\n\ndbutils.widgets.text(\"CATALOG\",defaultValue=\"edav_prd_cdh\")\nCATALOG=dbutils.widgets.get(\"CATALOG\")\ndbutils.widgets.text('SCHEMA',defaultValue='cdh_ml')\nSCHEMA=dbutils.widgets.get('SCHEMA')\n"
```

serviceName="notebook" and action_name="runCommand"
```python
**linkId**: "536877914"**
notebookId: "3511325195377517"
clusterId: "0109-184947-l0ka6b1y"
executionTime: "0.214"
status: "finished"
commandLanguage: "python"
commandId: "7359211675261069"
commandText: "import pyspark.pandas as ps\nfrom pyspark.sql import DataFrame\nimport pandas as p\n\n#pd=spark.sql(\"\"\"select * from cdh_truveta.condition limit 10 \"\"\")\ng=spark.table(\"cdh_truveta.condition\").limit(100)\n#p=ps.DataFrame(pd)\n#tb=spark.read.table(\"cdh_truveta.condition\")\n#tb.createOrReplaceTempView(\"dt\")\n\nx=isinstance(g, p.DataFrame)"
```

#[Databricks Column Lineage](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/lineage#column-lineage-table)
## Scope: all tables and column usage for EDAV_PRD_CDH catalog since inception[?]
Catalog, Schema, Table and Column values: source for queries, source and target for table creation
## Granularity
* the view/update for table (column is future)
## Facts
*  Command Facts (facts of interest **bolded**)




## Entity Metadata
job_info:
  job_id: "string"
  job_run_id: "string"
dashboard_id: "string"
legacy_dashboard_id: "string"
notebook_id: "string"
sql_query_id: "string"
dlt_pipeline_info:
  dlt_pipeline_id: "string"
  dlt_update_id: "string"