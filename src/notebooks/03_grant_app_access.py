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

dbutils.widgets.text("catalog", "tko_2026")
dbutils.widgets.text("schema", "lakebase_demo")
dbutils.widgets.text("lakebase_project", "tko-2026-demo")
dbutils.widgets.text("lakebase_db", "tko_2026_demo")
dbutils.widgets.text("app_name", "tko-insurance-app")

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
# MAGIC ## Create Lakebase OAuth Role for SP

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType
)

existing_roles = list(w.postgres.list_roles(
    parent=f"projects/{project_name}/branches/production"
))

sp_role_exists = False
for role in existing_roles:
    if (hasattr(role, 'status') and role.status
        and getattr(role.status, 'postgres_role', None) == sp_client_id
        and getattr(role.status, 'auth_method', None) == RoleAuthMethod.LAKEBASE_OAUTH_V1):
        print(f"OAuth role already exists for SP: {role.name}")
        sp_role_exists = True
        break

if not sp_role_exists:
    # Clean up any stale NO_LOGIN roles for this SP
    for role in existing_roles:
        if (hasattr(role, 'status') and role.status
            and getattr(role.status, 'postgres_role', None) == sp_client_id):
            print(f"Deleting stale role: {role.name}")
            w.postgres.delete_role(name=role.name).wait()

    print(f"Creating Lakebase OAuth role for SP: {sp_client_id}")
    op = w.postgres.create_role(
        parent=f"projects/{project_name}/branches/production",
        role=Role(
            spec=RoleRoleSpec(
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                postgres_role=sp_client_id,
            )
        ),
        role_id=f"sp-{app_name}",
    )
    result = op.wait()
    print(f"Created role: {result.name}")
    print(f"  Auth: {result.status.auth_method}, Identity: {result.status.identity_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Schema and Table Access to SP

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

cur.execute(f'GRANT USAGE ON SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA lakebase_demo GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp_client_id}"')

print(f"Granted full access on lakebase_demo to SP: {sp_client_id}")

cur.execute("SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = %s", (sp_client_id,))
print(f"Postgres role: {cur.fetchone()}")
conn.close()

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

print(f"Updating app.yaml with discovered resource IDs...")
print(f"  DBSQL_WAREHOUSE_ID = {warehouse_id}")
print(f"  LAKEBASE_HOST = {lakebase_host}")
print(f"  LAKEBASE_DB = {lakebase_db}")
print(f"  LAKEBASE_ENDPOINT = {lakebase_endpoint}")
print(f"  UC_CATALOG = {catalog}")
print(f"  UC_SCHEMA = {schema}")

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
"""

# Update the app.yaml in workspace
if source_code_path:
    import base64
    w.workspace.import_(
        path=f"{source_code_path}/app.yaml",
        content=base64.b64encode(app_yaml_content.encode()).decode(),
        format="AUTO",
        overwrite=True,
    )
    print(f"Updated app.yaml at {source_code_path}/app.yaml")

    # Redeploy the app with updated config
    print("Redeploying app with updated configuration...")
    w.apps.deploy(app_name=app_name, source_code_path=source_code_path)
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
