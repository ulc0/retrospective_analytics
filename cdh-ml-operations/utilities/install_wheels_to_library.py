# Databricks notebook source
# MAGIC %md
# MAGIC # Install databricks-cli

# COMMAND ----------

# MAGIC %pip install databricks-cli

# COMMAND ----------

# MAGIC %md
# MAGIC # install wheels to library
# MAGIC This script doesn't depend on a specific Databricks Runtime version. It only makes API calls
# MAGIC to the control plane.

# COMMAND ----------

import argparse
import base64
import json
import requests

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk.api_client import ApiClient



LEGACY_GLOBAL_INIT_PATH = "dbfs:/databricks/init"
BACKUP_PATH = "dbfs:/databricks/legacy-init-backup"


def getHeaders(token):
  return {'Authorization': 'Bearer {}'.format(token)}


def backupInitScript(dbfs_api, dbfs_path, dry_run):
  backup_dbfs_path = f"{BACKUP_PATH}/{dbfs_path.basename}"
  if dry_run:
    print(f"Dry run: Would copy {dbfs_path} to {backup_dbfs_path}")
  else:
    print(f"Copying {dbfs_path} to {backup_dbfs_path}")
    dbfs_api.cp(
      recursive = False,
      overwrite = True,
      src = dbfs_path.absolute_path,
      dst = backup_dbfs_path,
    )
  return DbfsPath(backup_dbfs_path)

def restoreInitScript(dbfs_api, dbfs_path, dry_run):
  restore_dbfs_path = f"{LEGACY_GLOBAL_INIT_PATH}/{dbfs_path.basename}"
  if dry_run:
    print(f"Dry run: Would copy {dbfs_path} to {restore_dbfs_path}")
  else:
    print(f"Copying {dbfs_path} to {restore_dbfs_path}")
    dbfs_api.cp(
      recursive = False,
      overwrite = True,
      src = dbfs_path.absolute_path,
      dst = restore_dbfs_path,
    )
  return DbfsPath(restore_dbfs_path)

def postFileToGlobalInitScriptAPI(dbfs_api, workspace, token, backup_dbfs_path, enable_v2_scripts, dry_run):
  tmp_dest = f"/tmp/{backup_dbfs_path.basename}"
  dbfs_api.get_file(dbfs_path=backup_dbfs_path, dst_path=tmp_dest, overwrite=True)
  with open(tmp_dest, 'rb') as localfile:
    contents = localfile.read()
    script_plaintext = "#COPIED BY global-init-script-disable.py\n" + contents.decode("utf-8")
    encoded = base64.b64encode(bytes(script_plaintext,"utf-8"))
    payload = {
      "name": backup_dbfs_path.basename,
      "script": encoded.decode("ascii"),
      "enabled": enable_v2_scripts
    }
    data = json.dumps(payload)

    api_url = f"{workspace}/api/2.0/global-init-scripts/"

    if dry_run:
      print(f"Dry run: Would POST {backup_dbfs_path} contents to {api_url}:\n{script_plaintext}")
      # Returns an "Accepted" status code
      return 202
    else:
      headers = getHeaders(token)
      print(f"POSTing {backup_dbfs_path} contents to {api_url}:\n{script_plaintext}")
      response = requests.request(
        "POST",
        api_url,
        data=data,
        headers=headers
      )
      print(response.text)
      return response.status_code

def enableScript(workspace,token,script_id,dry_run):
  api_url = f"{workspace}/api/2.0/global-init-scripts/{script_id}"
  headers = getHeaders(token)

  print(f"GETting {script_id} contents to {api_url}")
  response = requests.request(
    "GET",
    api_url,
    headers=headers
  )

  result = json.loads(response.text)
  decoded = base64.b64decode(result['script']).decode().splitlines()[0]

  if decoded == '#COPIED BY global-init-script-disable.py':
    payload = {
      "enabled": True
    }
    data = json.dumps(payload)

    if dry_run:
      print(f"Dry run: Would PATCH {script_id} to {api_url}")
      # Returns an "Accepted" status code
      return 202
    else:
      print(f"PATCHing {script_id} contents to {api_url}")
      response = requests.request(
        "PATCH",
        api_url,
        data=data,
        headers=headers
      )
      return response.status_code
  else:
    print(f"Global init script {script_id} is not copied by global-init-script-disable.py.")
    return 202

def disableScript(workspace,token,script_id,dry_run):
  api_url = f"{workspace}/api/2.0/global-init-scripts/{script_id}"
  headers = getHeaders(token)

  print(f"GETting {script_id} contents to {api_url}")
  response = requests.request(
    "GET",
    api_url,
    headers=headers
  )

  result = json.loads(response.text)
  decoded = base64.b64decode(result['script']).decode().splitlines()[0]

  if decoded == '#COPIED BY global-init-script-disable.py':
    payload = {
      "enabled": False
    }
    data = json.dumps(payload)

    if dry_run:
      print(f"Dry run: Would PATCH {script_id} to {api_url}")
      # Returns an "Accepted" status code
      return 202
    else:
      print(f"PATCHing {script_id} contents to {api_url}")
      response = requests.request(
        "PATCH",
        api_url,
        data=data,
        headers=headers
      )
      return response.status_code
  else:
    return 202

def disableLegacyInitScripts(workspace, token, dry_run):
  api_url = f"{workspace}/api/2.0/preview/workspace-conf"

  payload = { "enableDeprecatedGlobalInitScripts": "false" }
  data = json.dumps(payload)
  if dry_run:
    print(f"Dry run: Would PATCH {api_url}:\n{data}")
    # Returns an "Accepted" status code
    return 202
  else:
    headers = getHeaders(token)
    print(f"PATCHed {api_url}:\n{data}")
    response = requests.request(
      "PATCH",
      api_url,
      data=data,
      headers=headers
    )
    print(response.text)
    return response.status_code

def enableLegacyInitScripts(workspace, token, dry_run):
  api_url = f"{workspace}/api/2.0/preview/workspace-conf"

  payload = { "enableDeprecatedGlobalInitScripts": "true" }
  data = json.dumps(payload)
  if dry_run:
    print(f"Dry run: Would PATCH {api_url}:\n{data}")
    # Returns an "Accepted" status code
    return 202
  else:
    headers = getHeaders(token)
    print(f"PATCHed {api_url}:\n{data}")
    response = requests.request(
      "PATCH",
      api_url,
      data=data,
      headers=headers
    )
    print(response.text)
    return response.status_code

def checkLegacyInitScripts(workspace, token):
  api_url = f"{workspace}/api/2.0/preview/workspace-conf?keys=enableDeprecatedGlobalInitScripts"

  headers = getHeaders(token)
  print(f"GET {api_url}")
  response = requests.request(
    "GET",
    api_url,
    headers=headers
  )
  print(response.text)
  if json.loads(response.text)["enableDeprecatedGlobalInitScripts"] == "false":
    return True
  else:
    return False

def getGlobalInitScripts(workspace, token):
  api_url = f"{workspace}/api/2.0/global-init-scripts/"

  headers = getHeaders(token)
  print(f"Geting the list of Global Init Scripts.")
  response = requests.request(
    "GET",
    api_url,
    headers=headers
  )
  return json.loads(response.text)

def revertChanges(dbfs_api,workspace,token,dry_run):
  restore_init_files = dbfs_api.list_files(DbfsPath(BACKUP_PATH))
  for restore_init_file in restore_init_files:
    restored_file_dbfs_path = restoreInitScript(dbfs_api, restore_init_file.dbfs_path, dry_run)
    print(f"Restored {restored_file_dbfs_path}")

  enableLegacyInitScripts(workspace, token, dry_run)

  v2_init_scripts = getGlobalInitScripts(workspace,token)
  if v2_init_scripts != {}:
    for v2_init_script in v2_init_scripts['scripts']:
      status = disableScript(workspace,token,v2_init_script['script_id'],dry_run)
      if status == 200:
        print(f"Disabled global init script {v2_init_script['script_id']}")
      else:
        print(f"Not disabled global init script {v2_init_script['script_id']}. HTTP Error code {status}.")
  else:
    print("There is no Global init script.")

def main(workspace, token, enable_v2_scripts, dry_run, remove_scripts, revert_changes):
  workspace = workspace.rstrip('/')
  if revert_changes:
    api_client = ApiClient(
      host = workspace,
      token = token
    )
    dbfs_api = DbfsApi(api_client)
    revertChanges(dbfs_api,workspace,token,dry_run)
  elif checkLegacyInitScripts(workspace,token):
    print("Legaty init script is already disabled.")
  else:
    api_client = ApiClient(
      host = workspace,
      token = token
    )
    dbfs_api = DbfsApi(api_client)
    dbfs_api.mkdirs(DbfsPath(BACKUP_PATH))
    legacy_count = 0
    # Fetch the list of files in /databricks/init
    legacy_init_files = dbfs_api.list_files(DbfsPath(LEGACY_GLOBAL_INIT_PATH))
    v2_init_scripts = getGlobalInitScripts(workspace,token)
    for legacy_init_file in legacy_init_files:
      to_copy = True
      if not legacy_init_file.is_dir:
        backup_file_dbfs_path = backupInitScript(dbfs_api, legacy_init_file.dbfs_path, dry_run)
        # Post the local copy's contents to the /global-init-scripts API
        if v2_init_scripts != {}:
          for v2_init_script in v2_init_scripts['scripts']:
            if backup_file_dbfs_path.basename == v2_init_script['name']:
              print(f"Global init script {backup_file_dbfs_path.basename} already exists.")
              to_copy = False
              if enable_v2_scripts:
                status = enableScript(workspace,token,v2_init_script['script_id'],dry_run)
                if status == 200:
                  print(f"Enabled global init script {backup_file_dbfs_path.basename} because enable-v2-scripts is set.")
                elif status == 202:
                  print(f"Skipped to enable global init script {backup_file_dbfs_path.basename}.")
                else:
                  print(f"Failed to enable global init script {backup_file_dbfs_path.basename}.")
                  print(f"HTTP Error code {status}.")
              break

        if to_copy:
          result = postFileToGlobalInitScriptAPI(dbfs_api, workspace, token, backup_file_dbfs_path, enable_v2_scripts, dry_run)
          legacy_count = legacy_count + 1

          # If the POST suceeded, delete the file under /databricks/init
          if result == 200 and remove_scripts and enable_v2_scripts:
            print(f"Deleting {legacy_init_file.dbfs_path}")
            dbfs_api.delete(legacy_init_file.dbfs_path, recursive=False)
          elif result == 200 and remove_scripts:
            print(f"Not deleting {legacy_init_file.dbfs_path} because enable-v2-scripts is not set.")
          else:
            print(f"Not deleting {legacy_init_file.dbfs_path}")

    # Enable v2 scripts by calling the workspace admin conf API
    if enable_v2_scripts:
      print(f"Disabling legacy global init scripts feature and enabling new scripts")
      print(f"We recommend restarting your clusters to ensure they are running the most recent scripts.")
      print(f"If you completely confident that all your global init scripts are idempotent,")
      print(f"then there is no need to restart all clusters.")
      disableLegacyInitScripts(workspace, token, dry_run)

    print(f"{legacy_count} legacy global init scripts have been copied.")

if __name__ == "__main__":
  parser = argparse.ArgumentParser(
    description=f'Backup legacy global init script files from {LEGACY_GLOBAL_INIT_PATH} to {BACKUP_PATH}. '
    )
  parser.add_argument('workspace', 
                      help='Workspace URL to call, e.g. https://example.cloud.databricks.com')
  parser.add_argument('token',
                      help="Personal Access Token for API calls")
  parser.add_argument('-e', '--enable-v2-scripts',
                      action='store_true',
                      help=f'Enables v2 scripts and disables legacy global init scripts')
  parser.add_argument('-d', '--dry-run', 
                      action='store_true', 
                      help='Do a dry run and only print what actions would be taken')
  parser.add_argument('-r', '--remove-scripts', 
                      action='store_true', 
                      help=f'Remove the original scripts in {LEGACY_GLOBAL_INIT_PATH}')
  parser.add_argument('-C', '--revert-changes',
                      action='store_true',
                      help=f'Revert changes by this script')

  try:
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell' or get_ipython().__class__.__name__ == 'DatabricksShell':
      dbutils.widgets.dropdown("Enable New Scripts", "False", ["False", "True"])
      dbutils.widgets.dropdown("Dry Run", "True", ["False", "True"])
      dbutils.widgets.dropdown("Remove Old Scripts", "False", ["False", "True"])
      dbutils.widgets.dropdown("Revert Changes", "False", ["False", "True"])
      dbutils.widgets.text("Scope Name", "dbs-scope-prod-kv-CDH")
      dbutils.widgets.text("Secret Name", "cdh-adb-adf-access-token-v1")

      scope_name = dbutils.widgets.get("Scope Name")
      key_name = dbutils.widgets.get("Secret Name")

      if scope_name == "" or key_name == "":
        print("Please enter the scope and secret name for your Personal Access Token")
      else:
        workspace = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
        token = dbutils.secrets.get(scope_name, key_name)
        if token == "":
          dbutils.notebook.exit("Please verify your scope and secret names. This notebook requires them in order to make changes to your workspace.")

        dry_run = dbutils.widgets.get("Dry Run") == "True"
        enable_v2_scripts = dbutils.widgets.get("Enable New Scripts") == "True"
        remove_script = dbutils.widgets.get("Remove Old Scripts") == "True"
        revert_changes = dbutils.widgets.get("Revert Changes") == "True"
        main(workspace, token, enable_v2_scripts, dry_run, remove_script,revert_changes)
  except NameError:
    # Expecting this is a standard python interpreter
    args = parser.parse_args()
    main(args.workspace, args.token, args.enable_v2_scripts, args.dry_run, args.remove_scripts, args.revert_changes)
