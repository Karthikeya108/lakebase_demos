# TKO 2026 - Insurance Data Explorer

Compare Databricks DBSQL (Lakehouse) vs Lakebase Autoscaling performance for transactional workloads. A Databricks App lets you browse and edit 10 insurance tables (~570K total records) through both engines side by side.

All resources (SQL warehouse, Lakebase project, UC tables, app configuration) are created and accessed using the Databricks identity of whoever runs the setup. No hardcoded IDs or credentials -- everything is discovered dynamically.

## Architecture

- **Lakehouse**: Delta tables in Unity Catalog, queried via DBSQL Statement Execution API through a Photon-enabled SQL warehouse (created by the setup job)
- **Lakebase**: PostgreSQL 17 tables in Lakebase Autoscaling, queried via psycopg3 with OAuth authentication (project created by the setup job)
- **App**: FastAPI + Tailwind CSS served as a Databricks App, configured automatically by the setup job

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
│   ├── main.py                       # FastAPI backend (DBSQL + Lakebase queries)
│   ├── requirements.txt              # Python dependencies
│   └── static/
│       └── index.html                # Frontend (Tailwind CSS, inline JS)
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

### What the Setup Job Does

The setup job runs 3 tasks sequentially, all under the executor's identity:

| Task | What it creates | What it outputs |
|------|----------------|-----------------|
| **01_setup_lakehouse** | SQL warehouse (Photon, auto-stop), UC catalog/schema, 10 Delta tables with data | `warehouse_id` (task value) |
| **02_setup_lakebase** | Lakebase Autoscaling project (PG 17), database, schema, Postgres tables with matching data | `lakebase_host`, `lakebase_endpoint` (task values) |
| **03_configure_app** | OAuth role for app SP, schema/table grants, updates app.yaml with discovered IDs, redeploys app | -- |

All tasks are **idempotent** -- re-running skips resources that already exist.

Task values flow between tasks:
- Task 01 discovers/creates the SQL warehouse and passes `warehouse_id` to task 03
- Task 02 discovers the Lakebase endpoint host and passes `lakebase_host` and `lakebase_endpoint` to task 03
- Task 03 reads all upstream values, grants the app's service principal access, and writes the final `app.yaml` with all discovered IDs

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

## App Features

- Toggle between **Lakehouse** (DBSQL) and **Lakebase** (Postgres) data sources
- Browse all 10 tables with a table selector
- Paginated data view (10 records per page)
- Inline cell editing (double-click to edit, Enter to save)
- Response time display comparing both engines

## Cleanup

```bash
databricks bundle destroy
```

This removes the DABs-managed resources (job, app). To fully clean up, also delete:
- SQL warehouse: via Databricks UI or `databricks warehouses delete <id>`
- Lakebase project: `databricks postgres delete-project projects/<project-name>`
- UC catalog/schema: `DROP SCHEMA <catalog>.<schema> CASCADE`
