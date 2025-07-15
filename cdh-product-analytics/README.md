### Product Analytics - Table Utilization

*   Query and user counts by table, dataset, and database
		- What we need to answer here
			- Number of users for a time range that queried the table (repeat for database)
			- Number of queries for a time range that for a table (repeat for database)
*	Costs by query and usage
      - total execution time for a command id/text for a time range


command text
		- tables
		- databases


Logical Data Model for product analytics given the silver table
 
Dims

user (email, rest for later)
notebook (notebookid, rest for later)
cluster (clusterid, rest for later)
commands (commandid, commandtext, notebookid)
 
FACT - Core Silver table
**user** 
	notebookid
		commandid			
			executiontime

FACT		
database
	table
		column (future)
		#of users
		#of notebooks


### Source Data Assets 
* ~~information_schema.tables~~~
* edav_project_audit_logs.cdh.column_lineage

#### Primary Data Assets **cdh_engineering_etl**
* analytics_cluster_silver - daily snapshot of notebooks with commandtext,commandid,execution_time,email,cluster_id
  
#### Analytics Data Assets **cdh_engineering_etl**
* analytics_cluster_cube_collected - usage by core values
* analytics_cluster_cube - core_usage commandtext mapped to schemas as the core BI "cube" data for further analytics
* analytics_cluster_core_usage - daily aggregation of all silver tables by notebookid, commandid
* ~~~analytics_cluster_schemas - daily extract of cdh_* table info (excluding cdh_premier) ~~~


### Entity Relationship Diagrams

```mermaid
flowchart TD
    SILVER_USAGE_TBL["`**cdh_engineering_etl.analytics_cluster_silver**`"
        date event_date 
        string commandtext
        string email
        string clusterid
        string notebookid
        string schema_name
        string table_name
        float execution_time
        string commandid
        string regex_pattern
    ]

    COLUMN_LINEAGE_TBL["`**edav_project_audit_logs.cdh.column_lineage**`"
        string table_schema
        string table_name
        string entitytype
    ]

    CORE_USAGE_TBL["`**cdh_ml.analytics_cluster_core_usage**`"
        string notebookid
        string clusterid
        string email
        string commandid
        string commandtext
        date event_date
        double runtime
    ]

    COMMANDTEXT_TBL["`**cdh_engineering_etl.product_analytics_commandtext**`"
    string notebookid
    string commandid
    string querytext
    string clean_querytext

    ]
    
    ANALYTICS_TBL_COLLECTED["`**cdh_engineering_etl.product_analytics_cube_collected**`"
    ]
 
SILVER_USAGE_TBL--*query_utilization_core_data*-->CORE_USAGE_TBL
CORE_USAGE_TBL--*query_utilization_commandtext*-->COMMANDTEXT_TBL
COLUMN_LINEAGE_TBL--*query_utilization_commandtext*-->COMMANDTEXT_TBL
COMMANDTEXT_TBL--*query_utilization_commandtext*-->NOTEBOOK_SCHEMAS_TBL



```
