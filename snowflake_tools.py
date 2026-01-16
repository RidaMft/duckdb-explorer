from __future__ import annotations
import os
import re
import json
from typing import Optional, Tuple, Dict
import logging

import pandas as pd
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger('duckapp')

# ---------------- State persistence (JSON) ----------------
STATE_DIR = os.path.join('config')
STATE_FILE = os.path.join(STATE_DIR, 'app_state.json')

def _ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)

_DEF_STATE = {
    "snowflake_profiles": {},   # name -> {account,user,role,warehouse,database,schema}
    "sqlalchemy_connections": {},  # name -> url
}

def load_state() -> Dict:
    _ensure_state_dir()
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE,'w',encoding='utf-8') as f:
            json.dump(_DEF_STATE, f, ensure_ascii=False, indent=2)
        return json.loads(json.dumps(_DEF_STATE))
    try:
        with open(STATE_FILE,'r',encoding='utf-8') as f:
            data = json.load(f)
        for k,v in _DEF_STATE.items():
            if k not in data:
                data[k] = v
        return data
    except Exception:
        return json.loads(json.dumps(_DEF_STATE))

def save_state(state: Dict) -> None:
    _ensure_state_dir()
    with open(STATE_FILE,'w',encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# --- Snowflake profile helpers

def list_sf_profiles(state: Dict) -> Dict[str, Dict[str, str]]:
    return dict(state.get('snowflake_profiles', {}))

def save_sf_profile(state: Dict, name: str, profile: Dict[str,str]) -> None:
    state.setdefault('snowflake_profiles', {})[name] = profile

def delete_sf_profile(state: Dict, name: str) -> None:
    state.get('snowflake_profiles', {}).pop(name, None)

# --- SQLAlchemy connections helpers

def list_sqlalchemy_connections(state: Dict) -> Dict[str, str]:
    return dict(state.get('sqlalchemy_connections', {}))

def save_sqlalchemy_connection(state: Dict, name: str, url: str) -> None:
    state.setdefault('sqlalchemy_connections', {})[name] = url

def delete_sqlalchemy_connection(state: Dict, name: str) -> None:
    state.get('sqlalchemy_connections', {}).pop(name, None)

# ---------------- Snowflake helpers ----------------

def sf_normalize_account(acc: str) -> str:
    if not acc:
        raise ValueError("Paramètre 'account' vide.")
    acc = acc.strip()
    acc = re.sub(r"^https?://", "", acc, flags=re.IGNORECASE)
    acc = acc.split("/")[0]
    acc = re.sub(r"\.snowflakecomputing\.com$", "", acc, flags=re.IGNORECASE)
    return acc


def sf_connect_externalbrowser(
    account: str,
    user: str,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    login_timeout: int = 120,
) -> SnowflakeConnection:
    acc = sf_normalize_account(account)
    ctx = snowflake.connector.connect(
        account=acc,
        user=user,
        authenticator="externalbrowser",
        login_timeout=login_timeout,
    )
    cs = ctx.cursor()
    try:
        if role:
            cs.execute(f'USE ROLE "{role}"')
        if warehouse:
            cs.execute(f'USE WAREHOUSE "{warehouse}"')
        if database:
            cs.execute(f'USE DATABASE "{database}"')
        if schema:
            cs.execute(f'USE SCHEMA "{schema}"')
    finally:
        cs.close()
    return ctx


def sf_current_context(ctx: SnowflakeConnection) -> Dict[str, str]:
    cs = ctx.cursor()
    try:
        cs.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_REGION()")
        u, r, w, d, s, reg = cs.fetchone()
        return {
            "user": u or "",
            "role": r or "",
            "warehouse": w or "",
            "database": d or "",
            "schema": s or "",
            "region": reg or "",
        }
    finally:
        cs.close()

# ---------- DataFrame normalization for Snowflake ----------
def _normalize_df_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    # Convert columns that contain non-Arrow-friendly Python objects (e.g., uuid.UUID,
    # dict/list/mixed objects) to strings, so pyarrow conversion in write_pandas succeeds.
    import numpy as np
    import uuid as _uuid
    from decimal import Decimal as _Decimal
    import datetime as _dt
    def _is_arrow_friendly_value(v):
        return (
            v is None or
            isinstance(v, (str, bytes, bool, int, float, _dt.date, _dt.datetime, _Decimal, np.integer, np.floating, np.bool_))
        )
    for col in df.columns:
        s = df[col]
        # If it's already a known good dtype, skip
        if pd.api.types.is_bool_dtype(s) or pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s) or pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_string_dtype(s):
            continue
        # Inspect a small sample for problematic types (uuid, dict, list, set, custom objects)
        sample = s.dropna().head(50)
        needs_cast = False
        for v in sample:
            if isinstance(v, _uuid.UUID):
                needs_cast = True; break
            if isinstance(v, (dict, list, tuple, set)):
                needs_cast = True; break
            if not _is_arrow_friendly_value(v):
                needs_cast = True; break
        if needs_cast:
            df[col] = s.astype(str)
    return df


def sf_run_query(ctx: SnowflakeConnection, sql: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    cs = ctx.cursor()
    try:
        cs.execute(sql)
        if max_rows is not None:
            rows = cs.fetchmany(max_rows)
            cols = [d[0] for d in cs.description] if cs.description else []
            return pd.DataFrame(rows, columns=cols)
        try:
            return cs.fetch_pandas_all()
        except Exception:
            rows = cs.fetchall()
            cols = [d[0] for d in cs.description] if cs.description else []
            return pd.DataFrame(rows, columns=cols)
    finally:
        cs.close()


def sf_list_tables(ctx: SnowflakeConnection, database: Optional[str] = None, schema: Optional[str] = None) -> pd.DataFrame:
    cs = ctx.cursor()
    try:
        if database:
            cs.execute(f'USE DATABASE "{database}"')
        if schema:
            cs.execute(f'USE SCHEMA "{schema}"')
        cs.execute("SHOW TABLES")
        rows = cs.fetchall()
        cols = [d[0] for d in cs.description]
        df = pd.DataFrame(rows, columns=cols)
        out_cols = ["database_name", "schema_name", "name", "kind", "rows", "bytes", "owner", "created_on"]
        for c in out_cols:
            if c not in df.columns:
                df[c] = None
        return df[out_cols]
    finally:
        cs.close()


def sf_copy_duckdb_to_snowflake(
    duck_con,
    ctx: SnowflakeConnection,
    duck_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    mode: str = "replace",
    chunk_rows: Optional[int] = None,
) -> Tuple[int, int]:
    cur = ctx.cursor()
    try:
        cur.execute(f'USE DATABASE "{target_database}"')
        cur.execute(f'USE SCHEMA "{target_schema}"')
    finally:
        cur.close()
    if chunk_rows is None:
        df = duck_con.execute(f'SELECT * FROM "{duck_table}"').fetchdf()
        df = _normalize_df_for_snowflake(df)
        logger.info("DuckDB -> Snowflake: table=%s rows=%d cols=%d", duck_table, len(df), len(df.columns))
        success, nchunks, nrows, _ = write_pandas(
            conn=ctx,
            df=df,
            table_name=target_table,
            database=target_database,
            schema=target_schema,
            chunk_size=None,
            overwrite=(mode == "replace"),
            auto_create_table=True,
            quote_identifiers=True,
        )
        if not success:
            raise RuntimeError("write_pandas a indiqué un échec.")
        return int(nrows), int(len(df.columns))

    total_rows = 0
    cols = 0
    offset = 0
    while True:
        batch_df = duck_con.execute(
            f'SELECT * FROM "{duck_table}" LIMIT {int(chunk_rows)} OFFSET {int(offset)}'
        ).fetchdf()
        if batch_df.empty:
            break
        batch_df = _normalize_df_for_snowflake(batch_df)
        cols = len(batch_df.columns)
        success, nchunks, nrows, _ = write_pandas(
            conn=ctx,
            df=batch_df,
            table_name=target_table,
            database=target_database,
            schema=target_schema,
            chunk_size=None,
            overwrite=(mode == "replace" and offset == 0),
            auto_create_table=(offset == 0),
            quote_identifiers=True,
        )
        if not success:
            raise RuntimeError("write_pandas (batch) a indiqué un échec.")
        total_rows += int(nrows)
        offset += int(chunk_rows)
    return total_rows, cols


def sf_copy_snowflake_to_duckdb(
    ctx: SnowflakeConnection,
    duck_con,
    src_database: str,
    src_schema: str,
    src_table: Optional[str] = None,
    sql: Optional[str] = None,
    duck_target_table: str = "sf_data",
    mode: str = "replace",
    batch_size: int = 100_000
) -> Tuple[int, int]:
    if not src_database or not src_schema:
        raise ValueError("src_database et src_schema sont requis.")
    if not src_table and not sql:
        raise ValueError("Fournir soit src_table, soit sql (SELECT).")

    query = sql if sql else f'SELECT * FROM "{src_database}"."{src_schema}"."{src_table}"'
    cs = ctx.cursor()
    rows_total = 0
    cols = 0
    first = True
    try:
        cs.execute(query)
        try:
            for df in cs.fetch_pandas_batches(size=batch_size):
                if df is None or df.empty:
                    continue
                cols = len(df.columns)
                _persist_duckdb(duck_con, duck_target_table, df, mode=("replace" if first else "append"))
                rows_total += len(df)
                first = False
        except AttributeError:
            while True:
                rows = cs.fetchmany(batch_size)
                if not rows:
                    break
                colnames = [d[0] for d in cs.description] if cs.description else []
                df = pd.DataFrame(rows, columns=colnames)
                cols = len(df.columns)
                _persist_duckdb(duck_con, duck_target_table, df, mode=("replace" if first else "append"))
                rows_total += len(df)
                first = False
    finally:
        cs.close()
    return rows_total, cols


def _persist_duckdb(duck_con, table_name: str, df: pd.DataFrame, mode: str = "replace"):
    tmp_view = f"_tmp_{table_name}"
    duck_con.register(tmp_view, df)
    try:
        ident = f'"{table_name}"'
        tmp = f'"{tmp_view}"'
        if mode == "replace":
            duck_con.execute(f"CREATE OR REPLACE TABLE {ident} AS SELECT * FROM {tmp}")
        elif mode == "append":
            duck_con.execute(f"CREATE TABLE IF NOT EXISTS {ident} AS SELECT * FROM {tmp}")
            duck_con.execute(f"INSERT INTO {ident} SELECT * FROM {tmp}")
        elif mode == "create":
            duck_con.execute(f"CREATE TABLE {ident} AS SELECT * FROM {tmp}")
        else:
            raise ValueError("mode doit être 'replace', 'append' ou 'create'")
    finally:
        try:
            duck_con.unregister(tmp_view)
        except Exception:
            pass
