"""Databricks App: Lakehouse vs Lakebase Performance Comparison"""
import os
import time
import socket
from typing import Any
from decimal import Decimal

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

app = FastAPI(title="Insurance Data Explorer")
w = WorkspaceClient()

# Config - all values set by the setup job via app.yaml env vars
DBSQL_WAREHOUSE_ID = os.environ["DBSQL_WAREHOUSE_ID"]
LAKEBASE_HOST = os.environ["LAKEBASE_HOST"]
LAKEBASE_DB = os.environ["LAKEBASE_DB"]
LAKEBASE_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "lakebase_demo")
UC_CATALOG = os.environ["UC_CATALOG"]
UC_SCHEMA = os.environ["UC_SCHEMA"]
LAKEBASE_ENDPOINT = os.environ["LAKEBASE_ENDPOINT"]
LAKEBASE_DATA_API_URL = os.environ["LAKEBASE_DATA_API_URL"]

TABLES = [
    "policy_types", "agents", "customers", "policies", "vehicles",
    "beneficiaries", "coverages", "premiums", "claims", "claim_payments",
]

VALID_SOURCES = "^(dbsql_statement|dbsql_connector|lakebase_pg|lakebase_dataapi)$"


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


# --- DBSQL via Statement Execution API ---


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
                if val is not None:
                    try:
                        if "." in val:
                            val = float(val)
                        else:
                            val = int(val)
                    except (ValueError, TypeError):
                        pass
                    if val == "true":
                        val = True
                    elif val == "false":
                        val = False
                row[col] = val
            rows.append(row)
    return columns, rows


# --- DBSQL via JDBC/ODBC Connector ---


def query_dbsql_connector(sql: str) -> tuple[list[str], list[dict]]:
    """Execute SQL via databricks-sql-connector (Thrift/JDBC)."""
    from databricks.sql import connect as dbsql_connect

    host = w.config.host.replace("https://", "")
    token_header = w.config.authenticate()
    token = token_header.get("Authorization", "").replace("Bearer ", "")

    conn = dbsql_connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{DBSQL_WAREHOUSE_ID}",
        access_token=token,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        rows = []
        for raw in raw_rows:
            row = {}
            for i, col in enumerate(columns):
                val = raw[i]
                if isinstance(val, Decimal):
                    val = float(val)
                row[col] = val
            rows.append(row)
        return columns, rows
    finally:
        conn.close()


# --- Lakebase via psycopg (Postgres wire protocol) ---


def _get_pg_connection():
    import psycopg

    cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = LAKEBASE_HOST
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
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
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    row[col] = val
                rows.append(row)
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


# --- Lakebase Data API (PostgREST) ---


def _get_oauth_token() -> str:
    """Get OAuth token for Data API requests."""
    headers = w.config.authenticate()
    return headers.get("Authorization", "").replace("Bearer ", "")


def query_lakebase_dataapi(
    table: str, pk_col: str, page_size: int, offset: int, user_token: str = ""
) -> tuple[list[str], list[dict], int]:
    """Query via Lakebase Data API (PostgREST). Returns columns, rows, total_count."""
    token = user_token or _get_oauth_token()
    base = LAKEBASE_DATA_API_URL.rstrip("/")
    url = f"{base}/{LAKEBASE_SCHEMA}/{table}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": "count=exact",
    }
    params = {
        "order": f"{pk_col}.asc",
        "limit": str(page_size),
        "offset": str(offset),
    }

    resp = httpx.get(url, headers=headers, params=params, timeout=30.0)
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"Data API error: {resp.text}")

    rows = resp.json()
    total = 0
    content_range = resp.headers.get("Content-Range", "")
    if "/" in content_range:
        total_str = content_range.split("/")[-1]
        if total_str != "*":
            total = int(total_str)

    columns = list(rows[0].keys()) if rows else []
    return columns, rows, total


def update_lakebase_dataapi(
    table: str, pk_col: str, pk_val: Any, column: str, value: Any, user_token: str = ""
) -> None:
    """Update a record via Lakebase Data API."""
    token = user_token or _get_oauth_token()
    base = LAKEBASE_DATA_API_URL.rstrip("/")
    url = f"{base}/{LAKEBASE_SCHEMA}/{table}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {f"{pk_col}": f"eq.{pk_val}"}
    body = {column: value}

    resp = httpx.patch(url, headers=headers, params=params, json=body, timeout=30.0)
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"Data API update error: {resp.text}")


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
    request: Request,
    table: str,
    source: str = Query(..., pattern=VALID_SOURCES),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    if table not in TABLES:
        raise HTTPException(404, f"Table '{table}' not found")

    offset = (page - 1) * page_size
    pk = PK_MAP[table]

    if source == "dbsql_statement":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        data_sql = f"SELECT * FROM {fqn} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_dbsql(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_dbsql(data_sql)
        elapsed = (time.time() - start) * 1000

    elif source == "dbsql_connector":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        data_sql = f"SELECT * FROM {fqn} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_dbsql_connector(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_dbsql_connector(data_sql)
        elapsed = (time.time() - start) * 1000

    elif source == "lakebase_pg":
        schema_table = f"{LAKEBASE_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {schema_table}"
        data_sql = f"SELECT * FROM {schema_table} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_lakebase(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_lakebase(data_sql)
        elapsed = (time.time() - start) * 1000

    else:  # lakebase_dataapi
        user_token = ""  # Use SP token; DB owner can't use Data API directly
        start = time.time()
        columns, rows, total = query_lakebase_dataapi(table, pk, page_size, offset, user_token)
        elapsed = (time.time() - start) * 1000

    # Convert non-serializable types
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif isinstance(v, Decimal):
                row[k] = float(v)
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
async def update_record(request: Request, req: UpdateRequest, source: str = Query(..., pattern=VALID_SOURCES)):
    if req.table not in TABLES:
        raise HTTPException(404, f"Table '{req.table}' not found")

    if source in ("dbsql_statement", "dbsql_connector"):
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{req.table}"
        val = f"'{req.value}'" if isinstance(req.value, str) else ("NULL" if req.value is None else str(req.value))
        pk_val = f"'{req.pk_value}'" if isinstance(req.pk_value, str) else str(req.pk_value)
        sql = f"UPDATE {fqn} SET {req.column} = {val} WHERE {req.pk_column} = {pk_val}"
        start = time.time()
        if source == "dbsql_statement":
            query_dbsql(sql)
        else:
            query_dbsql_connector(sql)
        elapsed = (time.time() - start) * 1000

    elif source == "lakebase_pg":
        schema_table = f"{LAKEBASE_SCHEMA}.{req.table}"
        sql = f"UPDATE {schema_table} SET {req.column} = %s WHERE {req.pk_column} = %s"
        start = time.time()
        execute_lakebase(sql, (req.value, req.pk_value))
        elapsed = (time.time() - start) * 1000

    else:  # lakebase_dataapi
        user_token = ""  # Use SP token; DB owner can't use Data API directly
        start = time.time()
        update_lakebase_dataapi(req.table, req.pk_column, req.pk_value, req.column, req.value, user_token)
        elapsed = (time.time() - start) * 1000

    return UpdateResponse(success=True, elapsed_ms=round(elapsed, 2), source=source)


# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")
