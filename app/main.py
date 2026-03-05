"""Databricks App: Lakehouse vs Lakebase Performance Comparison"""
import os
import time
import socket
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

app = FastAPI(title="Insurance Data Explorer")
w = WorkspaceClient()

# Config
DBSQL_WAREHOUSE_ID = os.getenv("DBSQL_WAREHOUSE_ID", "040fbcbc0f11bbaa")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "ep-old-rice-ean8eftx.database.northeurope.azuredatabricks.net")
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "tko_2026_demo")
LAKEBASE_SCHEMA = "lakebase_demo"
UC_CATALOG = "tko_2026"
UC_SCHEMA = "lakebase_demo"
LAKEBASE_ENDPOINT = os.getenv(
    "LAKEBASE_ENDPOINT",
    "projects/tko-2026-demo/branches/production/endpoints/primary",
)

TABLES = [
    "policy_types", "agents", "customers", "policies", "vehicles",
    "beneficiaries", "coverages", "premiums", "claims", "claim_payments",
]


class TableDataResponse(BaseModel):
    table: str
    columns: list[str]
    rows: list[dict[str, Any]]
    total_count: int
    page: int
    page_size: int
    elapsed_ms: float
    source: str


class UpdateRequest(BaseModel):
    table: str
    pk_column: str
    pk_value: Any
    column: str
    value: Any


class UpdateResponse(BaseModel):
    success: bool
    elapsed_ms: float
    source: str


# --- DBSQL (Lakehouse) via Statement Execution API ---


def query_dbsql(sql: str) -> tuple[list[str], list[dict]]:
    """Execute SQL via Databricks SDK Statement Execution API."""
    from databricks.sdk.service.sql import StatementState

    resp = w.statement_execution.execute_statement(
        warehouse_id=DBSQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state == StatementState.FAILED:
        error_msg = resp.status.error.message if resp.status.error else "Unknown error"
        raise HTTPException(500, f"DBSQL error: {error_msg}")

    manifest = resp.manifest
    result = resp.result
    if not manifest or not manifest.schema or not manifest.schema.columns:
        return [], []

    columns = [col.name for col in manifest.schema.columns]
    rows = []
    if result and result.data_array:
        for row_data in result.data_array:
            row = {}
            for i, col in enumerate(columns):
                val = row_data[i] if i < len(row_data) else None
                # Try to convert numeric strings
                if val is not None:
                    try:
                        if "." in val:
                            val = float(val)
                        else:
                            val = int(val)
                    except (ValueError, TypeError):
                        pass
                    # Convert boolean strings
                    if val == "true":
                        val = True
                    elif val == "false":
                        val = False
                row[col] = val
            rows.append(row)
    return columns, rows


# --- Lakebase ---

def _get_pg_connection():
    import psycopg

    # Use SDK statement execution for Lakebase too if psycopg fails
    cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    # In Databricks Apps, current_user returns the SP identity
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = LAKEBASE_HOST
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host  # fallback to hostname directly
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def query_lakebase(sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return columns, rows
    finally:
        conn.close()


def execute_lakebase(sql: str, params: tuple | None = None) -> int:
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


# --- Primary key mapping ---

PK_MAP = {
    "policy_types": "policy_type_id",
    "agents": "agent_id",
    "customers": "customer_id",
    "policies": "policy_id",
    "vehicles": "vehicle_id",
    "beneficiaries": "beneficiary_id",
    "coverages": "coverage_id",
    "premiums": "premium_id",
    "claims": "claim_id",
    "claim_payments": "payment_id",
}


# --- API Endpoints ---

@app.get("/api/tables")
async def list_tables():
    return {"tables": TABLES}


@app.get("/api/data", response_model=TableDataResponse)
async def get_table_data(
    table: str,
    source: str = Query(..., pattern="^(lakehouse|lakebase)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    if table not in TABLES:
        raise HTTPException(404, f"Table '{table}' not found")

    offset = (page - 1) * page_size

    if source == "lakehouse":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        data_sql = f"SELECT * FROM {fqn} ORDER BY {PK_MAP[table]} LIMIT {page_size} OFFSET {offset}"

        start = time.time()
        _, count_rows = query_dbsql(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_dbsql(data_sql)
        elapsed = (time.time() - start) * 1000
    else:
        schema_table = f"{LAKEBASE_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {schema_table}"
        data_sql = f"SELECT * FROM {schema_table} ORDER BY {PK_MAP[table]} LIMIT {page_size} OFFSET {offset}"

        start = time.time()
        _, count_rows = query_lakebase(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_lakebase(data_sql)
        elapsed = (time.time() - start) * 1000

    # Convert non-serializable types
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif isinstance(v, bytes):
                row[k] = v.hex()

    return TableDataResponse(
        table=table,
        columns=columns,
        rows=rows,
        total_count=total,
        page=page,
        page_size=page_size,
        elapsed_ms=round(elapsed, 2),
        source=source,
    )


@app.post("/api/update", response_model=UpdateResponse)
async def update_record(req: UpdateRequest, source: str = Query(..., pattern="^(lakehouse|lakebase)$")):
    if req.table not in TABLES:
        raise HTTPException(404, f"Table '{req.table}' not found")

    if source == "lakehouse":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{req.table}"
        val = f"'{req.value}'" if isinstance(req.value, str) else ("NULL" if req.value is None else str(req.value))
        pk_val = f"'{req.pk_value}'" if isinstance(req.pk_value, str) else str(req.pk_value)
        sql = f"UPDATE {fqn} SET {req.column} = {val} WHERE {req.pk_column} = {pk_val}"
        start = time.time()
        query_dbsql(sql)
        elapsed = (time.time() - start) * 1000
    else:
        schema_table = f"{LAKEBASE_SCHEMA}.{req.table}"
        sql = f"UPDATE {schema_table} SET {req.column} = %s WHERE {req.pk_column} = %s"
        start = time.time()
        execute_lakebase(sql, (req.value, req.pk_value))
        elapsed = (time.time() - start) * 1000

    return UpdateResponse(success=True, elapsed_ms=round(elapsed, 2), source=source)


# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")
