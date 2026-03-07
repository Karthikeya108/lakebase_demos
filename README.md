# TKO 2026 - Lakebase Autoscaling Demos

Interactive demos showcasing Databricks Lakebase Autoscaling capabilities. A Databricks App provides hands-on exploration of data access patterns, database branching, and more — all built on an insurance dataset with ~570K records across 10 tables.

All resources (SQL warehouse, Lakebase project, UC tables, app configuration) are created and accessed using the Databricks identity of whoever runs the setup. No hardcoded IDs or credentials — everything is discovered dynamically.

## Demos

### 1. DBSQL vs Lakebase

Compare performance across four data access methods side by side:

| Method | Engine | Protocol | Description |
|--------|--------|----------|-------------|
| **DBSQL Statement API** | Lakehouse | REST | Databricks Statement Execution API via SDK |
| **DBSQL Connector** | Lakehouse | Thrift | databricks-sql-connector (JDBC/ODBC) |
| **Lakebase Postgres** | Lakebase | Postgres wire | psycopg3 with OAuth authentication |
| **Lakebase Data API** | Lakebase | REST (PostgREST) | RESTful HTTP interface with OAuth |

Features:
- Browse all 10 tables with paginated data view (10 records per page)
- Inline cell editing (double-click to edit, Enter to save)
- Prominent response time display comparing all four access methods

### 2. Branching Workflows

Demonstrate Lakebase's Git-like database branching with copy-on-write storage:

- **Create** a database branch (`dev-demo`) from the production branch
- **Run actions** on the branch only: +10% premium increase, delete cancelled policies, add new agent
- **Compare** production and branch data side by side with diff highlighting
- **Reset** the branch from production to discard all changes and start fresh

The branch and its compute endpoint are created/discovered dynamically via the SDK. No hardcoded endpoint names.

### Coming Soon

- Reverse ETL (synced tables from Delta Lake)
- Scale-to-Zero (autoscaling cost savings)
- Row-Level Security (PostgreSQL RLS via Data API)
- Point-in-Time Restore (instant recovery)

## Architecture

- **Lakehouse**: Delta tables in Unity Catalog, queried via a Photon-enabled SQL warehouse
- **Lakebase**: PostgreSQL 17 tables in Lakebase Autoscaling, queried via psycopg3 or the Data API
- **Branching**: Copy-on-write database branches with isolated compute endpoints
- **App**: FastAPI + Tailwind CSS served as a Databricks App

## Data Model

| Table | Rows | Description |
|-------|------|-------------|
| `policy_types` | 10 | Insurance product types |
| `agents` | 500 | Insurance agents |
| `customers` | 50,000 | Policyholders |
| `policies` | 100,000 | Insurance policies |
| `vehicles` | 60,000 | Insured vehicles |
| `beneficiaries` | 80,000 | Policy beneficiaries |
| `coverages` | 100,000 | Coverage details |
| `premiums` | 100,000 | Premium payments |
| `claims` | 80,000 | Insurance claims |
| `claim_payments` | 100,000 | Claim payment records |

## Project Structure

```
tko_2026/
├── databricks.yml                    # DABs main config (variables, targets)
├── resources/
│   ├── insurance_app.app.yml         # Databricks App resource definition
│   └── setup_job.yml                 # 3-task setup job definition
├── src/notebooks/
│   ├── 01_setup_lakehouse.py         # Creates SQL warehouse + UC catalog/schema + Delta tables
│   ├── 02_setup_lakebase.py          # Creates Lakebase project/DB + Postgres tables
│   └── 03_grant_app_access.py        # Grants app SP access + configures app env vars
├── app/                              # FastAPI app source
│   ├── app.yaml                      # App runtime config (populated by setup job)
│   ├── main.py                       # FastAPI backend (data access + branching)
│   ├── requirements.txt              # Python dependencies
│   └── static/
│       ├── home.html                 # Home page with demo cards
│       ├── index.html                # DBSQL vs Lakebase demo
│       └── branching.html            # Branching workflow demo
└── populate_lakebase.py              # Standalone script for manual Lakebase population
```

## Quick Start

### Prerequisites

- Databricks CLI configured with a profile
- Access to an Azure Databricks workspace with Unity Catalog enabled
- Permissions to create catalogs, SQL warehouses, Lakebase projects, and apps

### Deploy Everything

```bash
# 1. Validate the bundle
databricks bundle validate

# 2. Deploy resources to workspace
databricks bundle deploy

# 3. Deploy and start the app (creates it with placeholder config)
databricks bundle run insurance_app

# 4. Run the setup job (creates all infrastructure, data, and configures the app)
databricks bundle run setup_insurance_demo
```

### Post-Deploy: Lakebase Data API Setup

The Data API requires additional manual steps after the setup job completes:

1. **Enable the Data API** in the Lakebase UI:
   - Navigate to your Lakebase project > Data API
   - Click **Enable Data API**

2. **Expose the `lakebase_demo` schema**:
   - On the Data API page, go to **Settings** > **Advanced settings**
   - Under **Exposed schemas**, add `lakebase_demo`
   - Click **Save**

3. **Create the SP role for Data API access** (run in the Lakebase SQL Editor):
   ```sql
   -- Create the SP role using the databricks_auth extension
   CREATE EXTENSION IF NOT EXISTS databricks_auth;
   SELECT databricks_create_role('<app-sp-client-id>', 'SERVICE_PRINCIPAL');

   -- Grant the SP role to the authenticator (required for Data API)
   GRANT "<app-sp-client-id>" TO authenticator;

   -- Grant schema and table access
   GRANT USAGE ON SCHEMA lakebase_demo TO "<app-sp-client-id>";
   GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lakebase_demo TO "<app-sp-client-id>";
   ```

   Replace `<app-sp-client-id>` with the app's service principal client ID (found via the Databricks Apps UI or `databricks apps get <app-name>`).

4. **Refresh the schema cache** in the Data API page after granting permissions.

5. **Update `LAKEBASE_DATA_API_URL`** in `app.yaml`:
   ```
   https://<lakebase-host>/api/2.0/workspace/<workspace-id>/rest/<database-name>
   ```
   Then redeploy the app: `databricks bundle deploy && databricks bundle run insurance_app`

> **Important**: The database owner (whoever created the Lakebase project) cannot use the Data API directly. The `authenticator` role cannot assume owner privileges. The app uses the service principal's token instead.

### What the Setup Job Does

The setup job runs 3 tasks sequentially, all under the executor's identity:

| Task | What it creates | What it outputs |
|------|----------------|-----------------|
| **01_setup_lakehouse** | SQL warehouse (Photon, auto-stop), UC catalog/schema, 10 Delta tables with data | `warehouse_id` (task value) |
| **02_setup_lakebase** | Lakebase Autoscaling project (PG 17), database, schema, Postgres tables with matching data | `lakebase_host`, `lakebase_endpoint` (task values) |
| **03_configure_app** | OAuth role for app SP, schema/table grants, updates app.yaml with discovered IDs, redeploys app | -- |

All tasks are **idempotent** — re-running skips resources that already exist.

### Configuration Variables

Defined in `databricks.yml`, overridable per target or at deploy time:

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `tko_2026` | Unity Catalog catalog name |
| `schema` | `lakebase_demo` | UC schema name |
| `lakebase_project` | `tko-2026-demo` | Lakebase Autoscaling project name |
| `lakebase_db` | `tko_2026_demo` | Lakebase Postgres database name |

Override at deploy time:

```bash
databricks bundle deploy -var="catalog=my_catalog" -var="lakebase_project=my-project"
```

### Targets

- **dev** (default) - Development mode, prefixes resource names with user
- **prod** - Production mode, fixed resource names

```bash
# Deploy to prod
databricks bundle deploy -t prod
databricks bundle run insurance_app -t prod
databricks bundle run setup_insurance_demo -t prod
```

## Branching Demo Details

The branching demo uses the Databricks SDK to manage Lakebase database branches:

- **Branch creation**: `w.postgres.create_branch()` creates a copy-on-write branch from production
- **Endpoint provisioning**: `w.postgres.create_endpoint()` creates a read-write compute endpoint on the branch (endpoints are not auto-created)
- **Dynamic discovery**: The branch endpoint host is discovered via `w.postgres.get_endpoint()` — no hardcoded hostnames
- **Branch reset**: Deletes the branch, recreates it from production with a fresh endpoint
- **Comparison queries**: Both production and branch are queried via psycopg3 (Postgres wire protocol)

Branch actions modify data only on the branch, leaving production untouched. The side-by-side comparison highlights cells that differ between production and the branch.

## Data API Notes

The Lakebase Data API uses a PostgREST-compatible REST interface. Key details:

- **URL format**: `{REST_ENDPOINT}/{schema}/{table}` (e.g., `.../rest/tko_2026_demo/lakebase_demo/policy_types`)
- **Authentication**: OAuth bearer token in the `Authorization` header
- **Pagination**: `?limit=N&offset=M` query parameters; use `Prefer: count=exact` header for total counts
- **Filtering**: PostgREST syntax (e.g., `?id=eq.5`, `?status=in.(ACTIVE,PENDING)`)
- **Updates**: HTTP PATCH with `?pk_col=eq.value` filter
- **Role setup**: SP roles must be created via `databricks_create_role()` (not the SDK) and explicitly granted to `authenticator`
- **DB owner limitation**: The database owner cannot use the Data API; use a service principal or non-owner user

## Cleanup

```bash
databricks bundle destroy
```

This removes the DABs-managed resources (job, app). To fully clean up, also delete:
- SQL warehouse: via Databricks UI or `databricks warehouses delete <id>`
- Lakebase project: `databricks postgres delete-project projects/<project-name>`
- UC catalog/schema: `DROP SCHEMA <catalog>.<schema> CASCADE`
