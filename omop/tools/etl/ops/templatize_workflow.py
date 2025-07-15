import os
import json
from datetime import datetime, timezone
import requests

branch_name = os.getenv("BRANCH_NAME", default="dev")
cdh_env = "prod" if branch_name in ["master","main"] else "dev"
pat_token = os.getenv("DATABRICKS_PAT_TOKEN", default="")
databricks_host_name = os.getenv("DATABRICKS_HOST_NAME", default="")
databricks_policy_id = os.getenv("DATABRICKS_POLICY_ID", default="")

if databricks_host_name == "":
    raise Exception("DATABRICKS_HOST_NAME is not set")
else:
    print(f"Using Databricks Host Name: {databricks_host_name}")

if databricks_policy_id == "":
    raise Exception("DATABRICKS_POLICY_ID is not set")
else:
    print(f"Using DATABRICKS_POLICY_ID : {databricks_policy_id}")


if pat_token == "" or len(pat_token) < 1:
    raise Exception("DATABRICKS_PAT_TOKEN is not set")

databricks_reset_api_url = f"https://{databricks_host_name}/api/2.1/jobs/reset" 
databricks_create_api_url = f"https://{databricks_host_name}/api/2.1/jobs/create" 

workflow_map_file_path = "etl/ops/workflow_databricks_map.json"
workflow_map_json = {}
with open(workflow_map_file_path, 'r') as file:
    workflow_map_json = json.load(file)

# Read all json files in the workflows directory
workflows = []
now_utc = datetime.now(timezone.utc)
today_utc = now_utc.date()

temp_output_dir = "etl/temp_output"
os.makedirs(temp_output_dir, exist_ok=True)


headers = { 
    "Authorization": f"Bearer {pat_token}", 
    "Content-Type": "application/json" 
    }

for root, dirs, files in os.walk("etl/workflows"):
    for file in files:
        if file.endswith(".json"):
            task_keys = []
            file_name = file.split(".")[0]
            job_id = workflow_map_json[cdh_env][file_name] if file_name in workflow_map_json[cdh_env] else None 
            print(f"Processing {file} with job_id: {job_id}")
            url = databricks_reset_api_url
            if job_id is None:
                print(f"Job ID not found for {file_name}")
                url = databricks_create_api_url
                raise Exception(f"Job ID not found for {file_name}")

            workflows.append(os.path.join(root, file))
            with open(os.path.join(root, file), "r") as f:
                content = f.read()
                content_json = json.loads(content)
                content_json["git_source"]["git_branch"] = branch_name

                tasks = content_json["tasks"]
                tasks = [task for task in tasks if task["task_key"] not in ["send_event_success", "send_event_failure"]]
                for task in tasks:
                    task_key = task["task_key"]
                    task_keys.append({"task_key": task_key})

                tasks.append({
                    "task_key": "send_event_success",
                    "depends_on": task_keys,
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                    "notebook_path": "/Workspace/CDH/Developers/etl/_common_publish_status_workspace",
                    "base_parameters": {
                        "success": "True"
                    },
                    "source": "WORKSPACE"
                    },
                    "job_cluster_key": "xform_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "webhook_notifications": {}
                })


                tasks.append({
                    "task_key": "send_event_failure",
                    "depends_on":task_keys,
                    "run_if": "AT_LEAST_ONE_FAILED",
                    "notebook_task": {
                    "notebook_path": "/Workspace/CDH/Developers/etl/_common_publish_status_workspace",
                    "base_parameters": {
                        "success": "False"
                    },
                    "source": "WORKSPACE"
                    },
                    "job_cluster_key": "xform_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "webhook_notifications": {}
                })

                job_clusters = content_json["job_clusters"]
                for cluster in job_clusters:
                    cluster["new_cluster"]["spark_env_vars"]["CDH_ENVIRONMENT"] = cdh_env
                    cluster["new_cluster"]["policy_id"] = databricks_policy_id

                tags = content_json["tags"]
                tags["costid"] = "OPHDST"
                tags["project"] = "CDH-OHDSI"
                tags["last_updated_utc"] = now_utc.strftime('%Y-%m-%d %H:%M')

                if content_json.get("run_as") is not None:
                    content_json["run_as"] = None

                final_json = {
                    "job_id": job_id,
                    "new_settings": content_json
                }

                #write content_json to temp_output_dir as json
                with open(os.path.join(temp_output_dir, file), "w") as f:
                    f.write(json.dumps(final_json))
                
                # update databricks workspace
                response = requests.post(url, headers=headers, json=final_json)
                # Check the response 
                if response.status_code == 200: 
                    print("\t Workflow updated successfully") 
                else: 
                    raise Exception(f"\t Failed to update workflow: {response.text}")


