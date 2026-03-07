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


# --- Branching Demo ---

LAKEBASE_PROJECT = LAKEBASE_ENDPOINT.split("/")[1]  # e.g. "tko-2026-demo"
BRANCH_ID = "dev-demo"
BRANCH_NAME = f"projects/{LAKEBASE_PROJECT}/branches/{BRANCH_ID}"

# Mutable branch connection state (discovered dynamically)
_branch_state: dict[str, Any] = {"host": None, "endpoint": None}


def _get_branch_pg_connection():
    """Connect to the dev branch endpoint via psycopg."""
    import psycopg

    if not _branch_state["endpoint"]:
        raise HTTPException(400, "No active database branch. Create one first.")

    cred = w.postgres.generate_database_credential(endpoint=_branch_state["endpoint"])
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = _branch_state["host"]
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def _query_branch(sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_branch_pg_connection()
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
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


def _execute_branch(sql: str, params: tuple | None = None) -> int:
    conn = _get_branch_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def _discover_branch() -> bool:
    """Check if the dev-demo branch exists and discover its endpoint. Returns True if active."""
    if _branch_state["endpoint"]:
        return True
    try:
        w.postgres.get_branch(name=BRANCH_NAME)
    except Exception:
        return False
    # Branch exists, discover endpoint
    endpoints = list(w.postgres.list_endpoints(parent=BRANCH_NAME))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        _branch_state["host"] = endpoints[0].status.hosts.host
        _branch_state["endpoint"] = endpoints[0].name
        return True
    return False


@app.get("/api/branch/status")
async def branch_status():
    active = _discover_branch()
    return {"active": active, "branch": BRANCH_ID}


def _create_branch_with_endpoint():
    """Create the branch and its compute endpoint. Returns the endpoint."""
    from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType

    # Create branch
    w.postgres.create_branch(
        parent=f"projects/{LAKEBASE_PROJECT}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{LAKEBASE_PROJECT}/branches/production",
                no_expiry=True,
            )
        ),
        branch_id=BRANCH_ID,
    ).wait()

    # Create compute endpoint on the branch
    w.postgres.create_endpoint(
        parent=BRANCH_NAME,
        endpoint=Endpoint(
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=1.0,
                autoscaling_limit_max_cu=1.0,
            )
        ),
        endpoint_id="primary",
    ).wait()

    # Discover the endpoint host
    ep = w.postgres.get_endpoint(name=f"{BRANCH_NAME}/endpoints/primary")
    _branch_state["host"] = ep.status.hosts.host
    _branch_state["endpoint"] = ep.name


@app.post("/api/branch/create")
async def create_branch():
    """Create the dev-demo database branch from production."""
    if _discover_branch():
        return {"status": "exists", "branch": BRANCH_ID, "message": "Database branch already exists"}

    start = time.time()
    _create_branch_with_endpoint()
    elapsed = (time.time() - start) * 1000

    return {
        "status": "created",
        "branch": BRANCH_ID,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Database branch created in {elapsed/1000:.1f}s",
    }


@app.get("/api/branch/compare")
async def compare_branches(
    table: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    if table not in TABLES:
        raise HTTPException(404, f"Table '{table}' not found")
    if not _discover_branch():
        raise HTTPException(400, "No active database branch")

    pk = PK_MAP[table]
    offset = (page - 1) * page_size
    schema_table = f"{LAKEBASE_SCHEMA}.{table}"
    count_sql = f"SELECT COUNT(*) AS cnt FROM {schema_table}"
    data_sql = f"SELECT * FROM {schema_table} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"

    # Query production via psycopg
    start = time.time()
    _, prod_count = query_lakebase(count_sql)
    prod_cols, prod_rows = query_lakebase(data_sql)
    prod_ms = (time.time() - start) * 1000

    # Query branch via psycopg
    start = time.time()
    _, branch_count = _query_branch(count_sql)
    branch_cols, branch_rows = _query_branch(data_sql)
    branch_ms = (time.time() - start) * 1000

    return {
        "table": table,
        "pk": pk,
        "page": page,
        "page_size": page_size,
        "production": {
            "columns": prod_cols,
            "rows": prod_rows,
            "total_count": prod_count[0]["cnt"],
            "elapsed_ms": round(prod_ms, 2),
        },
        "branch": {
            "columns": branch_cols,
            "rows": branch_rows,
            "total_count": branch_count[0]["cnt"],
            "elapsed_ms": round(branch_ms, 2),
        },
    }


class BranchActionRequest(BaseModel):
    action: str  # "premium_increase", "delete_cancelled", "add_agent"
    table: str = "premiums"


@app.post("/api/branch/action")
async def branch_action(req: BranchActionRequest):
    if not _discover_branch():
        raise HTTPException(400, "No active database branch")
    start = time.time()

    if req.action == "premium_increase":
        affected = _execute_branch(
            f"UPDATE {LAKEBASE_SCHEMA}.premiums SET amount = ROUND(amount * 1.10, 2)"
        )
        message = f"Increased all premiums by 10% ({affected} rows updated)"

    elif req.action == "delete_cancelled":
        affected = _execute_branch(
            f"DELETE FROM {LAKEBASE_SCHEMA}.policies WHERE status = 'CANCELLED'"
        )
        message = f"Deleted all cancelled policies ({affected} rows removed)"

    elif req.action == "add_agent":
        cols, rows = _query_branch(f"SELECT MAX(agent_id) AS max_id FROM {LAKEBASE_SCHEMA}.agents")
        next_id = (rows[0]["max_id"] or 0) + 1
        _execute_branch(
            f"INSERT INTO {LAKEBASE_SCHEMA}.agents (agent_id, first_name, last_name, email, phone, license_number, hire_date, region, is_active) "
            f"VALUES (%s, 'Demo', 'Agent', 'demo.agent@insurance.com', '555-0000', 'LIC-DEMO-001', CURRENT_DATE, 'Central', true)",
            (next_id,),
        )
        affected = 1
        message = f"Added new agent 'Demo Agent' (ID: {next_id})"

    else:
        raise HTTPException(400, f"Unknown action: {req.action}")

    elapsed = (time.time() - start) * 1000
    return {"success": True, "message": message, "affected_rows": affected, "elapsed_ms": round(elapsed, 2)}


@app.post("/api/branch/reset")
async def reset_branch():
    """Delete and recreate the dev branch from production to reset all data."""
    start = time.time()

    # Clear state
    _branch_state["host"] = None
    _branch_state["endpoint"] = None

    # Delete existing branch
    try:
        w.postgres.delete_branch(name=BRANCH_NAME).wait()
    except Exception as e:
        if "NOT_FOUND" not in str(e):
            raise HTTPException(500, f"Failed to delete branch: {e}")

    # Recreate branch + endpoint
    _create_branch_with_endpoint()

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Database branch reset from production in {elapsed/1000:.1f}s",
    }


# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def home():
    return FileResponse("static/home.html")


@app.get("/dbsql-vs-lakebase")
async def dbsql_vs_lakebase():
    return FileResponse("static/index.html")


@app.get("/branching")
async def branching():
    return FileResponse("static/branching.html")
