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


# --- Reverse ETL Demo ---
# Manual copy approach: read from Delta via DBSQL, write to Lakebase via psycopg.
# Demonstrates the data flow pattern without requiring a UC online catalog.

RETL_TABLE = "analytics_output"
RETL_FQN = f"{UC_CATALOG}.{UC_SCHEMA}.{RETL_TABLE}"
RETL_PG_TABLE = f"{LAKEBASE_SCHEMA}.{RETL_TABLE}"
RETL_PK = "id"

# Track whether the sync target table exists in Lakebase
_retl_state: dict[str, Any] = {"target_exists": False}


def _check_retl_source() -> bool:
    """Check if the source Delta table exists."""
    try:
        query_dbsql(f"DESCRIBE TABLE {RETL_FQN}")
        return True
    except Exception:
        return False


def _check_retl_target() -> bool:
    """Check if the target Lakebase table exists."""
    try:
        query_lakebase(f"SELECT 1 FROM {RETL_PG_TABLE} LIMIT 1")
        _retl_state["target_exists"] = True
        return True
    except Exception:
        _retl_state["target_exists"] = False
        return False


@app.get("/api/retl/status")
async def retl_status():
    """Check if source table and target Lakebase table exist."""
    source_exists = _check_retl_source()
    target_exists = _check_retl_target()

    # Get row counts
    src_count = 0
    tgt_count = 0
    if source_exists:
        try:
            _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
            src_count = rows[0]["cnt"]
        except Exception:
            pass
    if target_exists:
        try:
            _, rows = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {RETL_PG_TABLE}")
            tgt_count = rows[0]["cnt"]
        except Exception:
            pass

    return {
        "source_exists": source_exists,
        "source_table": RETL_FQN,
        "source_count": src_count,
        "synced": target_exists,
        "target_table": RETL_PG_TABLE,
        "target_count": tgt_count,
        "sync_status": {
            "state": "SYNCED" if target_exists and src_count == tgt_count else
                     "OUT_OF_SYNC" if target_exists and src_count != tgt_count else
                     "NOT_CREATED",
            "message": f"{tgt_count}/{src_count} rows synced" if target_exists else "Target table not created",
        } if source_exists else None,
    }


@app.post("/api/retl/setup-source")
async def retl_setup_source():
    """Create the source Delta table with sample data."""
    start = time.time()

    query_dbsql(f"""
        CREATE TABLE IF NOT EXISTS {RETL_FQN} (
            id BIGINT,
            report_date DATE,
            region STRING,
            total_policies INT,
            total_premium DECIMAL(12,2),
            avg_claim_amount DECIMAL(10,2)
        )
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Check if empty and populate
    _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
    if rows[0]["cnt"] == 0:
        query_dbsql(f"""
            INSERT INTO {RETL_FQN} VALUES
            (1, '2026-03-01', 'Northeast', 12450, 8234500.00, 3245.50),
            (2, '2026-03-01', 'Southeast', 9870, 6123400.00, 2987.25),
            (3, '2026-03-01', 'Midwest', 11200, 7456700.00, 3102.80),
            (4, '2026-03-01', 'West', 15600, 10234500.00, 3567.90),
            (5, '2026-03-01', 'Central', 8900, 5678900.00, 2876.40),
            (6, '2026-03-02', 'Northeast', 12500, 8267800.00, 3256.10),
            (7, '2026-03-02', 'Southeast', 9920, 6156700.00, 2995.50),
            (8, '2026-03-02', 'Midwest', 11250, 7489000.00, 3115.20),
            (9, '2026-03-02', 'West', 15650, 10278900.00, 3578.30),
            (10, '2026-03-02', 'Central', 8950, 5701200.00, 2889.60)
        """)

    elapsed = (time.time() - start) * 1000
    return {"success": True, "elapsed_ms": round(elapsed, 2), "message": "Source table created with 10 rows"}


@app.post("/api/retl/create-sync")
async def retl_create_sync():
    """Create the target table in Lakebase and perform initial sync from Delta."""
    if _check_retl_target():
        return {"status": "exists", "message": "Target table already exists in Lakebase"}

    start = time.time()

    # Create the table in Lakebase
    execute_lakebase(f"""
        CREATE TABLE IF NOT EXISTS {RETL_PG_TABLE} (
            id BIGINT PRIMARY KEY,
            report_date DATE,
            region TEXT,
            total_policies INTEGER,
            total_premium NUMERIC(12,2),
            avg_claim_amount NUMERIC(10,2)
        )
    """)

    # Read all data from Delta
    _, src_rows = query_dbsql(f"SELECT * FROM {RETL_FQN} ORDER BY {RETL_PK}")

    # Insert into Lakebase
    if src_rows:
        conn = _get_pg_connection()
        try:
            with conn.cursor() as cur:
                for row in src_rows:
                    cur.execute(
                        f"INSERT INTO {RETL_PG_TABLE} (id, report_date, region, total_policies, total_premium, avg_claim_amount) "
                        f"VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                        (row["id"], row["report_date"], row["region"],
                         row["total_policies"], row["total_premium"], row["avg_claim_amount"]),
                    )
                conn.commit()
        finally:
            conn.close()

    _retl_state["target_exists"] = True
    elapsed = (time.time() - start) * 1000
    return {
        "status": "created",
        "elapsed_ms": round(elapsed, 2),
        "message": f"Synced {len(src_rows)} rows from Delta to Lakebase in {elapsed/1000:.1f}s",
    }


@app.post("/api/retl/trigger-sync")
async def retl_trigger_sync():
    """Sync new rows from Delta to Lakebase (incremental upsert)."""
    if not _retl_state["target_exists"] and not _check_retl_target():
        raise HTTPException(400, "No target table in Lakebase. Create one first.")

    start = time.time()

    # Get max ID already in Lakebase
    _, tgt_rows = query_lakebase(f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {RETL_PG_TABLE}")
    max_id = tgt_rows[0]["max_id"]

    # Read new rows from Delta
    _, new_rows = query_dbsql(
        f"SELECT * FROM {RETL_FQN} WHERE id > {max_id} ORDER BY {RETL_PK}"
    )

    synced = 0
    if new_rows:
        conn = _get_pg_connection()
        try:
            with conn.cursor() as cur:
                for row in new_rows:
                    cur.execute(
                        f"INSERT INTO {RETL_PG_TABLE} (id, report_date, region, total_policies, total_premium, avg_claim_amount) "
                        f"VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                        (row["id"], row["report_date"], row["region"],
                         row["total_policies"], row["total_premium"], row["avg_claim_amount"]),
                    )
                conn.commit()
                synced = len(new_rows)
        finally:
            conn.close()

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "synced_rows": synced,
        "message": f"Synced {synced} new rows to Lakebase" if synced > 0 else "Already in sync — no new rows",
    }


@app.post("/api/retl/delete-sync")
async def retl_delete_sync():
    """Drop the target table in Lakebase."""
    if not _retl_state["target_exists"] and not _check_retl_target():
        return {"status": "not_found", "message": "No target table in Lakebase"}

    start = time.time()
    try:
        execute_lakebase(f"DROP TABLE IF EXISTS {RETL_PG_TABLE}")
    except Exception as e:
        raise HTTPException(500, f"Failed to drop table: {e}")

    _retl_state["target_exists"] = False
    elapsed = (time.time() - start) * 1000
    return {"status": "deleted", "elapsed_ms": round(elapsed, 2), "message": "Target table dropped from Lakebase"}


@app.get("/api/retl/compare")
async def retl_compare(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    """Compare source Delta table and target Lakebase table."""
    offset = (page - 1) * page_size

    # Query source via DBSQL
    start = time.time()
    _, src_count = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
    src_cols, src_rows = query_dbsql(
        f"SELECT * FROM {RETL_FQN} ORDER BY {RETL_PK} LIMIT {page_size} OFFSET {offset}"
    )
    src_ms = (time.time() - start) * 1000

    # Query target via psycopg
    tgt_cols, tgt_rows, tgt_total, tgt_ms = [], [], 0, 0.0
    if _retl_state["target_exists"] or _check_retl_target():
        try:
            start = time.time()
            _, tgt_count = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {RETL_PG_TABLE}")
            tgt_cols, tgt_rows = query_lakebase(
                f"SELECT * FROM {RETL_PG_TABLE} ORDER BY {RETL_PK} LIMIT {page_size} OFFSET {offset}"
            )
            tgt_total = tgt_count[0]["cnt"]
            tgt_ms = (time.time() - start) * 1000
            for row in tgt_rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif isinstance(v, Decimal):
                        row[k] = float(v)
        except Exception:
            pass

    return {
        "source": {
            "columns": src_cols,
            "rows": src_rows,
            "total_count": src_count[0]["cnt"],
            "elapsed_ms": round(src_ms, 2),
        },
        "target": {
            "columns": tgt_cols,
            "rows": tgt_rows,
            "total_count": tgt_total,
            "elapsed_ms": round(tgt_ms, 2),
        },
        "page": page,
        "page_size": page_size,
    }


@app.post("/api/retl/insert")
async def retl_insert():
    """Insert a new row into the source Delta table."""
    import random

    start = time.time()

    _, rows = query_dbsql(f"SELECT MAX(id) AS max_id FROM {RETL_FQN}")
    next_id = (rows[0]["max_id"] or 0) + 1

    regions = ["Northeast", "Southeast", "Midwest", "West", "Central"]
    region = regions[(next_id - 1) % len(regions)]

    policies = random.randint(5000, 20000)
    premium = round(random.uniform(3000000, 12000000), 2)
    avg_claim = round(random.uniform(2000, 4000), 2)

    query_dbsql(f"""
        INSERT INTO {RETL_FQN} VALUES
        ({next_id}, CURRENT_DATE(), '{region}', {policies}, {premium}, {avg_claim})
    """)

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Inserted row {next_id} ({region}, {policies} policies, ${premium:,.2f} premium)",
    }


# --- Scale-to-Zero Demo ---

# Query latency history: list of {elapsed_ms, state, timestamp}
_s2z_history: list[dict[str, Any]] = []


def _get_endpoint_info() -> dict[str, Any]:
    """Get the production endpoint status and config."""
    ep = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT)
    status = ep.status
    spec = ep.spec

    # Determine state
    state = "UNKNOWN"
    if status and status.current_state:
        state = str(status.current_state.value) if hasattr(status.current_state, "value") else str(status.current_state)
        # Clean up enum prefix
        state = state.replace("ENDPOINT_STATE_", "")

    # Suspend timeout — lives in status, not spec
    suspend_timeout = 0
    for src in [status, spec]:
        if src and getattr(src, "suspend_timeout_duration", None):
            raw = src.suspend_timeout_duration
            if hasattr(raw, "seconds"):
                suspend_timeout = int(raw.seconds)
            elif isinstance(raw, str) and raw.endswith("s"):
                suspend_timeout = int(raw.rstrip("s"))
            elif isinstance(raw, (int, float)):
                suspend_timeout = int(raw)
            if suspend_timeout > 0:
                break

    return {
        "state": state,
        "endpoint_name": ep.name,
        "min_cu": float(status.autoscaling_limit_min_cu) if status and status.autoscaling_limit_min_cu else 0,
        "max_cu": float(status.autoscaling_limit_max_cu) if status and status.autoscaling_limit_max_cu else 0,
        "suspend_timeout_seconds": suspend_timeout,
        "scale_to_zero_enabled": suspend_timeout > 0,
    }


@app.get("/api/s2z/status")
async def s2z_status():
    """Get endpoint state, config, and query history."""
    info = _get_endpoint_info()
    return {
        **info,
        "history": _s2z_history[-20:],  # last 20 queries
    }


@app.post("/api/s2z/configure")
async def s2z_configure(timeout_seconds: int = Query(..., ge=0)):
    """Set the scale-to-zero suspend timeout. 0 = disabled (always active)."""
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, FieldMask
    from google.protobuf.duration_pb2 import Duration

    start = time.time()

    # Get current endpoint config to preserve CU settings
    current = _get_endpoint_info()

    spec_kwargs: dict[str, Any] = {
        "endpoint_type": EndpointType.ENDPOINT_TYPE_READ_WRITE,
        "autoscaling_limit_min_cu": current["min_cu"] or 1.0,
        "autoscaling_limit_max_cu": current["max_cu"] or 1.0,
    }
    if timeout_seconds > 0:
        spec_kwargs["suspend_timeout_duration"] = Duration(seconds=timeout_seconds)
    else:
        spec_kwargs["no_suspension"] = True

    w.postgres.update_endpoint(
        name=LAKEBASE_ENDPOINT,
        endpoint=Endpoint(
            name=LAKEBASE_ENDPOINT,
            spec=EndpointSpec(**spec_kwargs),
        ),
        update_mask=FieldMask(field_mask=["spec"]),
    ).wait()

    elapsed = (time.time() - start) * 1000
    label = f"{timeout_seconds}s" if timeout_seconds > 0 else "disabled"
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Scale-to-zero timeout set to {label}",
    }


@app.post("/api/s2z/query")
async def s2z_query():
    """Run a test query and record the latency. On cold start, runs a second
    warm query to measure wake-up overhead separately."""
    import datetime

    # Get current state before query
    info = _get_endpoint_info()
    pre_state = info["state"]
    is_cold = pre_state == "SUSPENDED"

    # Run the query (includes wake-up time if suspended)
    sql = f"SELECT COUNT(*) AS cnt FROM {LAKEBASE_SCHEMA}.policies"
    start = time.time()
    query_lakebase(sql)
    elapsed = (time.time() - start) * 1000

    # On cold start, immediately run a second warm query to isolate wake-up time
    warm_ms = None
    wakeup_ms = None
    if is_cold:
        start2 = time.time()
        query_lakebase(sql)
        warm_ms = round((time.time() - start2) * 1000, 2)
        wakeup_ms = round(elapsed - warm_ms, 2)

    entry = {
        "elapsed_ms": round(elapsed, 2),
        "pre_state": pre_state,
        "cold_start": is_cold,
        "warm_ms": warm_ms,
        "wakeup_ms": wakeup_ms,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    _s2z_history.append(entry)

    return entry


@app.post("/api/s2z/clear-history")
async def s2z_clear_history():
    """Clear the query latency history."""
    _s2z_history.clear()
    return {"success": True}


# --- Row-Level Security Demo ---

RLS_TABLE = "agents"
RLS_SCHEMA_TABLE = f"{LAKEBASE_SCHEMA}.{RLS_TABLE}"
RLS_PK = "agent_id"
RLS_REGIONS = ["Central", "Midwest", "Northeast", "Northwest", "South", "Southeast", "Southwest", "West"]
RLS_PERSONAS = {r: f"rls_{r.lower()}" for r in RLS_REGIONS}  # region -> pg role

# Track whether RLS has been set up on this app instance
_rls_state: dict[str, Any] = {"enabled": True}  # Pre-configured via setup script


def _query_lakebase_as_role(sql: str, role: str | None = None) -> tuple[list[str], list[dict]]:
    """Query Lakebase, optionally as a specific Postgres role."""
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            if role:
                cur.execute(f"SET ROLE {role}")
            else:
                cur.execute("RESET ROLE")
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


@app.get("/api/rls/status")
async def rls_status():
    """Get RLS status and available personas."""
    return {
        "enabled": _rls_state["enabled"],
        "table": RLS_SCHEMA_TABLE,
        "personas": [
            {"region": r, "role": RLS_PERSONAS[r]} for r in RLS_REGIONS
        ],
    }


@app.get("/api/rls/query")
async def rls_query(
    persona: str = Query("admin"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    """Query agents table as a specific persona."""
    offset = (page - 1) * page_size
    role = None
    if persona != "admin" and persona in RLS_PERSONAS:
        role = RLS_PERSONAS[persona]

    count_sql = f"SELECT COUNT(*) AS cnt FROM {RLS_SCHEMA_TABLE}"
    data_sql = f"SELECT * FROM {RLS_SCHEMA_TABLE} ORDER BY {RLS_PK} LIMIT {page_size} OFFSET {offset}"

    start = time.time()
    _, count_rows = _query_lakebase_as_role(count_sql, role)
    columns, rows = _query_lakebase_as_role(data_sql, role)
    elapsed = (time.time() - start) * 1000

    return {
        "persona": persona,
        "role": role or "owner (bypasses RLS)",
        "columns": columns,
        "rows": rows,
        "total_count": count_rows[0]["cnt"],
        "page": page,
        "page_size": page_size,
        "elapsed_ms": round(elapsed, 2),
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


@app.get("/reverse-etl")
async def reverse_etl():
    return FileResponse("static/reverse-etl.html")


@app.get("/scale-to-zero")
async def scale_to_zero():
    return FileResponse("static/scale-to-zero.html")


@app.get("/row-level-security")
async def row_level_security():
    return FileResponse("static/row-level-security.html")
