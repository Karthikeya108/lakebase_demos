# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Configure App - Grant SP Access & Set Environment
# MAGIC Discovers the app's service principal, creates a Lakebase OAuth role,
# MAGIC grants schema/table access, and configures the app with discovered resource IDs.
# MAGIC All operations use the executor's identity.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.0" "databricks-sdk>=0.81.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "lakebase_demos")
dbutils.widgets.text("schema", "lakebase_demo")
dbutils.widgets.text("lakebase_project", "lakebase-demos")
dbutils.widgets.text("lakebase_db", "lakebase_demos")
dbutils.widgets.text("app_name", "lakebase-demos-app")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
project_name = dbutils.widgets.get("lakebase_project")
lakebase_db = dbutils.widgets.get("lakebase_db")
app_name = dbutils.widgets.get("app_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Discovered Values from Upstream Tasks

# COMMAND ----------

# Read values discovered by upstream tasks
warehouse_id = dbutils.jobs.taskValues.get(
    taskKey="01_setup_lakehouse", key="warehouse_id"
)
lakebase_host = dbutils.jobs.taskValues.get(
    taskKey="02_setup_lakebase", key="lakebase_host"
)
lakebase_endpoint = dbutils.jobs.taskValues.get(
    taskKey="02_setup_lakebase", key="lakebase_endpoint"
)

print(f"Warehouse ID (from task 01): {warehouse_id}")
print(f"Lakebase host (from task 02): {lakebase_host}")
print(f"Lakebase endpoint (from task 02): {lakebase_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover App Service Principal

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
print(f"Running as: {me.user_name}")

app = w.apps.get(name=app_name)
sp_id = str(app.service_principal_id)
print(f"App: {app_name}")
print(f"App SP ID: {sp_id}")

sp = w.service_principals.get(id=sp_id)
sp_client_id = sp.application_id
print(f"App SP Client ID: {sp_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant CAN_MANAGE on Lakebase Project to App SP
# MAGIC
# MAGIC The branching demo needs to create/delete branches via the Lakebase
# MAGIC Postgres API, which requires CAN_MANAGE on the database project. This
# MAGIC isn't a supported App resource type, so we set it via the Permissions API.

# COMMAND ----------

from databricks.sdk.service import iam

w.permissions.set(
    request_object_type="database-projects",
    request_object_id=project_name,
    access_control_list=[
        iam.AccessControlRequest(
            service_principal_name=sp_client_id,
            permission_level=iam.PermissionLevel.CAN_MANAGE,
        ),
    ],
)
print(f"Granted CAN_MANAGE on Lakebase project '{project_name}' to SP {sp_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up the SP's Postgres Role and Grants
# MAGIC
# MAGIC Create the SP's Lakebase OAuth role via the `databricks_auth` SQL
# MAGIC helper, grant it membership in `authenticator` so the Data API can
# MAGIC impersonate it, and grant it schema/table access.
# MAGIC
# MAGIC These statements run via psycopg under the executor's identity. The
# MAGIC executor is the role's admin, which is required to subsequently
# MAGIC `GRANT "<sp>" TO authenticator`.

# COMMAND ----------

import psycopg
import socket

cred = w.postgres.generate_database_credential(endpoint=lakebase_endpoint)
username = me.user_name or me.display_name

try:
    ip = socket.gethostbyname(lakebase_host)
except Exception:
    ip = lakebase_host

conn = psycopg.connect(
    host=lakebase_host, hostaddr=ip, dbname=lakebase_db,
    user=username, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")

# Create the SP role only if it doesn't already exist. Skips quietly on re-runs.
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_client_id,))
if not cur.fetchone():
    cur.execute(
        "SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')",
        (sp_client_id,),
    )
    print(f"Created Lakebase OAuth role for SP: {sp_client_id}")
else:
    print(f"OAuth role already exists for SP: {sp_client_id}")

# Allow the Data API's `authenticator` role to impersonate the SP.
cur.execute(f'GRANT "{sp_client_id}" TO authenticator')

# Schema/table grants
cur.execute(f'GRANT USAGE, CREATE ON SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA lakebase_demo GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp_client_id}"')

print(f"Granted full access on lakebase_demo to SP: {sp_client_id}")

cur.execute("SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = %s", (sp_client_id,))
print(f"Postgres role: {cur.fetchone()}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Unity Catalog Access to App SP
# MAGIC
# MAGIC The app SP needs to read/write Delta tables via DBSQL. UC SCHEMA is NOT a
# MAGIC supported App resource securable type, so we issue these grants via SQL
# MAGIC instead. Requires the executor to have grant authority on the catalog.

# COMMAND ----------

uc_grants = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp_client_id}`",
    f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `{sp_client_id}`",
    f"GRANT SELECT ON SCHEMA `{catalog}`.`{schema}` TO `{sp_client_id}`",
    # Required for the OLAP benchmark which CREATEs `premium_transactions`
    f"GRANT CREATE TABLE ON SCHEMA `{catalog}`.`{schema}` TO `{sp_client_id}`",
    f"GRANT MODIFY ON SCHEMA `{catalog}`.`{schema}` TO `{sp_client_id}`",
]
for stmt in uc_grants:
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=stmt, wait_timeout="30s"
        )
        state = resp.status.state.value if resp.status and resp.status.state else "?"
        if state != "SUCCEEDED":
            err = resp.status.error.message if resp.status and resp.status.error else "unknown"
            print(f"  WARN: {stmt} -> {state}: {err}")
        else:
            print(f"  OK: {stmt}")
    except Exception as e:
        print(f"  FAILED: {stmt} -> {e}")
        print(f"  If the executor lacks grant authority, ask your catalog admin to run the above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update App Environment Variables
# MAGIC Configures the app with the dynamically discovered warehouse ID, Lakebase host, etc.

# COMMAND ----------

import json

# Get current app config
app_config = w.apps.get(name=app_name)
source_code_path = None
if app_config.active_deployment:
    source_code_path = app_config.active_deployment.source_code_path

lakebase_data_api_url = f"https://{lakebase_host}/api/2.0/workspace/{w.get_workspace_id()}/rest/{lakebase_db}"

print(f"Updating app.yaml with discovered resource IDs...")
print(f"  DBSQL_WAREHOUSE_ID = {warehouse_id}")
print(f"  LAKEBASE_HOST = {lakebase_host}")
print(f"  LAKEBASE_DB = {lakebase_db}")
print(f"  LAKEBASE_ENDPOINT = {lakebase_endpoint}")
print(f"  UC_CATALOG = {catalog}")
print(f"  UC_SCHEMA = {schema}")
print(f"  LAKEBASE_DATA_API_URL = {lakebase_data_api_url}")

# Write updated app.yaml to the app source directory
app_yaml_content = f"""command:
  - uvicorn
  - main:app
  - --host
  - 0.0.0.0
  - --port
  - "8000"

env:
  - name: DBSQL_WAREHOUSE_ID
    value: "{warehouse_id}"
  - name: LAKEBASE_HOST
    value: "{lakebase_host}"
  - name: LAKEBASE_DB
    value: "{lakebase_db}"
  - name: LAKEBASE_ENDPOINT
    value: "{lakebase_endpoint}"
  - name: UC_CATALOG
    value: "{catalog}"
  - name: UC_SCHEMA
    value: "{schema}"
  - name: LAKEBASE_DATA_API_URL
    value: "{lakebase_data_api_url}"
"""

# Update the app.yaml in workspace
if source_code_path:
    import base64
    from databricks.sdk.service.workspace import ImportFormat
    w.workspace.import_(
        path=f"{source_code_path}/app.yaml",
        content=base64.b64encode(app_yaml_content.encode()).decode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    print(f"Updated app.yaml at {source_code_path}/app.yaml")

    # Update the App's `resources` field so the platform auto-grants the SP
    # CAN_USE on the SQL warehouse. Done as an additive update — any
    # other resources the user has attached to the app via the UI (serving
    # endpoints, secrets, etc.) are preserved; we replace just the entry
    # named "sql-warehouse". (UC SCHEMA isn't a supported App resource
    # securable type — only TABLE/VOLUME/FUNCTION/CONNECTION — so we don't
    # declare it here. UC table grants for the SP are issued separately above.)
    print("Updating app resources (warehouse CAN_USE)...")
    from databricks.sdk.service.apps import (
        App,
        AppResource,
        AppResourceSqlWarehouse,
        AppResourceSqlWarehouseSqlWarehousePermission,
    )
    existing_resources = list(app_config.resources or [])
    other_resources = [r for r in existing_resources if r.name != "sql-warehouse"]
    new_resources = other_resources + [
        AppResource(
            name="sql-warehouse",
            description="DBSQL warehouse for Lakehouse queries",
            sql_warehouse=AppResourceSqlWarehouse(
                id=warehouse_id,
                permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE,
            ),
        ),
    ]
    w.apps.create_update(
        app_name=app_name,
        update_mask="resources",
        app=App(name=app_name, resources=new_resources),
    ).result()
    print(f"App resources updated ({len(other_resources)} preserved + 1 sql-warehouse).")

    # Redeploy the app with updated config
    print("Redeploying app with updated configuration...")
    from databricks.sdk.service.apps import AppDeployment
    w.apps.deploy(
        app_name=app_name,
        app_deployment=AppDeployment(source_code_path=source_code_path),
    )
    print("App redeployment triggered.")
else:
    print("WARNING: No active deployment found. Deploy the app first, then re-run this task.")
    print("Generated app.yaml content:")
    print(app_yaml_content)

# COMMAND ----------

print(f"\n{'='*50}")
print("App Configuration Complete")
print(f"{'='*50}")
print(f"App: {app_name}")
print(f"SP: {sp_client_id}")
print(f"Warehouse: {warehouse_id}")
print(f"Lakebase: {lakebase_host} / {lakebase_db}")
print(f"UC: {catalog}.{schema}")
