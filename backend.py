
"""
Backend pour l'application Streamlit, DuckDB persistant + extensions.

Fonctionnalités :
- Connexion DuckDB persistante
- Chargement CSV robuste (séparateurs multi-caractères) + options low_memory/dtype
- Import CSV streamé en chunks → DuckDB avec callback de progression et coercions
- Ingestion SQLAlchemy (Postgres/MySQL/SQL Server...) en chunks avec callback,
  pré-comptage et coercions (ex. forcer siret en texte)
- Exécution/validation SQL (SELECT/CTE uniquement)
- Profilage de table (types, NULLs, fréquences, stats numériques)
- Catalogue de requêtes (save/load/delete + clone + export/import)
- Atelier (SELECT avec alias/WHERE + comparaison de jeux)
- ATTACH extensions Postgres/MySQL/SQLite
- Export de tables (Parquet/CSV)
- Système de logs (RotatingFileHandler + console)
"""
from __future__ import annotations

from typing import Optional, List, Dict, Tuple
import re
import io
import os
import logging
from logging.handlers import RotatingFileHandler

import pandas as pd
import duckdb
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def init_logging(log_dir: str = 'logs', level: int = logging.INFO) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger('duckapp')
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s - %(message)s')
        file_path = os.path.join(log_dir, 'app.log')
        fh = RotatingFileHandler(file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
        fh.setLevel(level)
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.propagate = False
    return logger

logger = logging.getLogger('duckapp')

# ---------------------------------------------------------------------------
# Connexion DuckDB
# ---------------------------------------------------------------------------

def make_duckdb_connection(db_path: str = "data/catalog.duckdb") -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(database=db_path)
    con.execute("PRAGMA enable_object_cache;")
    try:
        n_threads = max(1, (os.cpu_count() or 4))
        con.execute(f"PRAGMA threads={int(n_threads)};")
    except Exception:
        pass
    return con

def quote_ident(name: str) -> str:
    parts = str(name).split('.')
    quoted = []
    for p in parts:
        q = str(p).replace('"', '""')
        quoted.append(f'"{q}"')
    return '.'.join(quoted)

def list_tables(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY table_schema, table_name
        """
    ).fetchdf()

def table_rowcount(con: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0])
    except Exception:
        return 0

def table_schema(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    return con.execute(f"PRAGMA table_info({quote_ident(table)})").fetchdf()

def persist_df(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame, mode: str = "replace") -> None:
    temp_view = f"_tmp_{table_name}"
    con.register(temp_view, df)
    ident = quote_ident(table_name)
    tmp = quote_ident(temp_view)
    if mode == "replace":
        con.execute(f"CREATE OR REPLACE TABLE {ident} AS SELECT * FROM {tmp}")
    elif mode == "append":
        con.execute(f"CREATE TABLE IF NOT EXISTS {ident} AS SELECT * FROM {tmp}")
        con.execute(f"INSERT INTO {ident} SELECT * FROM {tmp}")
    elif mode == "create":
        con.execute(f"CREATE TABLE {ident} AS SELECT * FROM {tmp}")
    else:
        raise ValueError("mode doit être 'replace', 'append' ou 'create'")
    try:
        con.unregister(temp_view)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Extensions DuckDB
# ---------------------------------------------------------------------------

def enable_extensions(con: duckdb.DuckDBPyConnection, install: bool = True) -> None:
    exts = ["postgres", "mysql", "sqlite", "httpfs", "json"]
    for e in exts:
        if install:
            try:
                con.execute(f"INSTALL {e} FROM core;")
            except Exception:
                try:
                    con.execute(f"INSTALL {e};")
                except Exception:
                    pass
        try:
            con.execute(f"LOAD {e};")
        except Exception:
            pass

def attach_external(
    con: duckdb.DuckDBPyConnection,
    db_type: str,
    conn_str: str,
    alias: str,
    schema: Optional[str] = None,
    read_only: bool = True
) -> None:
    db_type = db_type.lower()
    ro = ", READ_ONLY" if read_only else ""
    extra = ""
    if db_type == 'postgres' and schema:
        extra = f", SCHEMA '{schema}'"

    if db_type == 'sqlite':
        con.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE sqlite{ro});")
    elif db_type in ('postgres', 'mysql'):
        con.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE {db_type}{ro}{extra});")
    else:
        raise ValueError("db_type doit être 'postgres', 'mysql' ou 'sqlite'")

# ---------------------------------------------------------------------------
# CSV helpers (multi-caractères)
# ---------------------------------------------------------------------------

_DEF_SINGLE_SEP = "\x1f"

def normalize_multichar_sep_stream(file_obj, sep_multi: str, sep_single: str = _DEF_SINGLE_SEP, encoding: str = "utf-8") -> io.BytesIO:
    data = file_obj.read()
    text = data.decode(encoding, errors='replace') if isinstance(data, (bytes, bytearray)) else data
    out_lines: List[str] = []
    for line in text.splitlines():
        buf: List[str] = []
        in_quotes = False
        i = 0
        L = len(line)
        while i < L:
            ch = line[i]
            if ch == '"':
                if i + 1 < L and line[i+1] == '"':
                    buf.append('""')
                    i += 2
                    continue
                in_quotes = not in_quotes
                buf.append(ch)
                i += 1
                continue
            if not in_quotes and line.startswith(sep_multi, i):
                buf.append(sep_single)
                i += len(sep_multi)
                continue
            buf.append(ch)
            i += 1
        out_lines.append(''.join(buf))
    normalized = '\n'.join(out_lines)
    return io.BytesIO(normalized.encode(encoding))

def load_csv_to_df(
    file_obj,
    sep: str = ",",
    encoding: str = "utf-8",
    header: Optional[int] = 0,
    quotechar: Optional[str] = '"',
    escapechar: Optional[str] = None,
    parse_mode: str = 'literal',
    low_memory: bool = False,
    dtype: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Lecture CSV en DataFrame (preview), avec support des séparateurs multi-caractères.
    """
    is_multichar = len(sep) > 1
    if is_multichar and parse_mode == 'literal':
        cleaned_stream = normalize_multichar_sep_stream(file_obj, sep_multi=sep, sep_single=_DEF_SINGLE_SEP, encoding=encoding)
        return pd.read_csv(
            cleaned_stream, sep=_DEF_SINGLE_SEP, encoding=encoding, header=header,
            quotechar=(quotechar if quotechar else '"'), escapechar=(escapechar if escapechar else None),
            engine='c', dtype=dtype, low_memory=low_memory
        )
    elif is_multichar and parse_mode == 'regex':
        return pd.read_csv(
            file_obj, sep=sep, encoding=encoding, header=header,
            quotechar=(quotechar if quotechar else '"'), escapechar=(escapechar if escapechar else None),
            engine='python', dtype=dtype, low_memory=low_memory
        )
    else:
        return pd.read_csv(
            file_obj, sep=sep, encoding=encoding, header=header,
            quotechar=(quotechar if quotechar else '"'), escapechar=(escapechar if escapechar else None),
            engine='c', dtype=dtype, low_memory=low_memory
        )

def _coerce_cols_to_string(df: pd.DataFrame, cols: Optional[List[str]]) -> None:
    if not cols:
        return
    for c in cols:
        if c in df.columns:
            s = df[c]
            # Coercion robuste : numérique → Int64 (nullable) → string ; sinon cast direct string
            try:
                s_num = pd.to_numeric(s, errors='coerce')
                s = s_num.astype('Int64').astype('string')
            except Exception:
                s = s.astype('string')
            df[c] = s

def _coerce_cols_to_bigint(df: pd.DataFrame, cols: Optional[List[str]]) -> None:
    if not cols:
        return
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')  # pandas nullable int → DuckDB BIGINT

def load_csv_to_duckdb_stream(
    con: duckdb.DuckDBPyConnection,
    file_obj,
    target_table: str,
    sep: str = ",",
    encoding: str = "utf-8",
    header: Optional[int] = 0,
    quotechar: Optional[str] = '"',
    escapechar: Optional[str] = None,
    parse_mode: str = 'literal',
    chunksize: int = 200_000,
    progress_cb = None,  # callback: (rows_total:int|None, rows_chunk:int, chunks_done:int) -> None
    mode: str = "replace",
    coerce_str_cols: Optional[List[str]] = None,
    coerce_bigint_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Lit un CSV en chunks et alimente directement DuckDB (évite un DF complet en RAM).
    Retourne un aperçu (<=200 lignes) pour l’affichage UI.
    """
    logger = logging.getLogger('duckapp')
    logger.info("Import CSV streamé: table=%s, chunksize=%d, mode=%s, str_cols=%s, bigint_cols=%s",
                target_table, chunksize, mode, coerce_str_cols, coerce_bigint_cols)

    # Re-normalise le flux si séparateur multi-caractères
    is_multichar = len(sep) > 1
    stream = normalize_multichar_sep_stream(file_obj, sep_multi=sep, sep_single=_DEF_SINGLE_SEP, encoding=encoding) \
        if (is_multichar and parse_mode == 'literal') else file_obj

    read_kwargs = dict(
        sep=(_DEF_SINGLE_SEP if (is_multichar and parse_mode == 'literal') else sep),
        encoding=encoding,
        header=(None if (header is None or header < 0) else header),
        quotechar=(quotechar if quotechar else '"'),
        escapechar=(escapechar if escapechar else None),
        chunksize=chunksize,
        engine=('python' if (is_multichar and parse_mode == 'regex') else 'c'),
        dtype=None,
    )

    chunks_done, total_rows = 0, 0
    first = True
    for chunk in pd.read_csv(stream, **read_kwargs):
        # coercions de types (ex. SIRET → string)
        _coerce_cols_to_string(chunk, coerce_str_cols)
        _coerce_cols_to_bigint(chunk, coerce_bigint_cols)

        rows_chunk = len(chunk)
        if first:
            persist_df(con, target_table, chunk, mode=mode)  # replace/create
            first = False
        else:
            persist_df(con, target_table, chunk, mode="append")
        chunks_done += 1
        total_rows += rows_chunk
        logger.info("CSV chunk %d importé (rows=%d, cumul=%d)", chunks_done, rows_chunk, total_rows)
        if progress_cb:
            progress_cb(None, rows_chunk, chunks_done)

    logger.info("Import CSV streamé terminé: table=%s, rows=%d, chunks=%d", target_table, total_rows, chunks_done)
    # Retourne un aperçu
    return con.execute(f"SELECT * FROM {quote_ident(target_table)} LIMIT 200").fetchdf()

# ---------------------------------------------------------------------------
# SQL validation & exécution
# ---------------------------------------------------------------------------

FORBIDDEN_KEYWORDS = [
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bDROP\b", r"\bALTER\b",
    r"\bCREATE\b", r"\bREPLACE\b", r"\bTRUNCATE\b", r"\bATTACH\b", r"\bDETACH\b",
    r"\bCOPY\b", r"\bVACUUM\b",
]

def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*", " ", sql)
    return sql

def validate_select_only(sql: str) -> Tuple[bool, str]:
    if not sql or not sql.strip():
        return False, "La requête est vide."
    cleaned = _strip_sql_comments(sql).strip()
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(kw, cleaned, flags=re.IGNORECASE):
            return False, "Seules les requêtes SELECT/CTE sont autorisées. Mot-clé interdit détecté."
    statements = [s.strip() for s in cleaned.split(';') if s.strip()]
    if not statements:
        return False, "Aucune instruction SQL trouvée."
    last_stmt = statements[-1]
    if re.match(r"^(SELECT|WITH)\b", last_stmt, flags=re.IGNORECASE):
        all_select_cte = all(re.match(r"^(SELECT|WITH)\b", s, flags=re.IGNORECASE) for s in statements)
        return (True, "OK") if all_select_cte else (False, "La requête doit être un SELECT ou des CTE terminant par un SELECT.")
    return False, "La requête doit être un SELECT ou des CTE terminant par un SELECT."

def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()

def explain_analyze(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        df = con.execute(f"EXPLAIN ANALYZE {sql}").fetchdf()
        if df.shape[1] == 1:
            return df
        return pd.DataFrame({"explain": ['\n'.join(map(str, row)) for row in df.values]})
    except Exception:
        return pd.DataFrame({"explain": []})

def profile_query(con: duckdb.DuckDBPyConnection, sql: str) -> Tuple[float, pd.DataFrame, pd.DataFrame]:
    import time
    start = time.perf_counter()
    result_df = run_query(con, sql)
    elapsed = time.perf_counter() - start
    exp_df = explain_analyze(con, sql)
    return elapsed, result_df, exp_df

# ---------------------------------------------------------------------------
# Profilage table
# ---------------------------------------------------------------------------

def profile_table(con: duckdb.DuckDBPyConnection, table: str, top_n: int = 5) -> Dict[str, pd.DataFrame]:
    ident = quote_ident(table)
    schema_df = table_schema(con, table)
    cols = schema_df['name'].tolist()

    # NULLs
    null_exprs = ', '.join([f"SUM(CASE WHEN {quote_ident(c)} IS NULL THEN 1 ELSE 0 END) AS {quote_ident(c+'_nulls')}" for c in cols])
    nulls_df = con.execute(f"SELECT {null_exprs} FROM {ident}").fetchdf().T.reset_index()
    nulls_df.columns = ['colonne', 'valeur']

    # Types
    num_types = {'SMALLINT','INTEGER','BIGINT','HUGEINT','UTINYINT','TINYINT','USMALLINT','UINTEGER','UBIGINT','FLOAT','DOUBLE','DECIMAL'}
    cat_types = {'BOOLEAN','DATE','TIME','TIMESTAMP','VARCHAR','BLOB'}

    num_cols = [row['name'] for _, row in schema_df.iterrows() if str(row['type']).upper().split('(')[0] in num_types]
    numeric_stats_df = pd.DataFrame()
    if num_cols:
        agg_parts = []
        for c in num_cols:
            ci = quote_ident(c)
            agg_parts += [
                f"COUNT({ci}) AS {quote_ident(c+'_count')}",
                f"MIN({ci}) AS {quote_ident(c+'_min')}",
                f"MAX({ci}) AS {quote_ident(c+'_max')}",
                f"AVG({ci}) AS {quote_ident(c+'_avg')}",
                f"STDDEV_POP({ci}) AS {quote_ident(c+'_std')}",
                f"COUNT(DISTINCT {ci}) AS {quote_ident(c+'_distinct')}"
            ]
        query = f"SELECT {', '.join(agg_parts)} FROM {ident}"
        numeric_stats_df = con.execute(query).fetchdf().T.reset_index()
        numeric_stats_df.columns = ['metric', 'value']

    cat_cols = [row['name'] for _, row in schema_df.iterrows() if str(row['type']).upper().split('(')[0] in cat_types]
    freq_frames: List[pd.DataFrame] = []
    for c in cat_cols:
        ci = quote_ident(c)
        q = f"SELECT {ci} AS valeur, COUNT(*) AS freq FROM {ident} GROUP BY {ci} ORDER BY freq DESC NULLS LAST LIMIT {top_n}"
        df = con.execute(q).fetchdf()
        df.insert(0, 'colonne', c)
        freq_frames.append(df)
    categorical_stats_df = pd.concat(freq_frames, ignore_index=True) if freq_frames else pd.DataFrame(columns=['colonne','valeur','freq'])

    return {'schema_df': schema_df, 'numeric_stats_df': numeric_stats_df, 'categorical_stats_df': categorical_stats_df, 'nulls_df': nulls_df}

# ---------------------------------------------------------------------------
# Catalogue de requêtes
# ---------------------------------------------------------------------------

def init_query_catalog(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_queries (
            name VARCHAR PRIMARY KEY,
            sql VARCHAR NOT NULL,
            tags VARCHAR,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        );
        """
    )

def save_query(con: duckdb.DuckDBPyConnection, name: str, sql: str, tags: Optional[str] = None) -> None:
    if not name or not sql:
        raise ValueError("Nom et SQL sont requis")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM meta_queries WHERE name = ?", [name])
        con.execute(
            "INSERT INTO meta_queries(name, sql, tags, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [name, sql, tags or None]
        )
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        raise e

def list_queries(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("SELECT name, tags, updated_at FROM meta_queries ORDER BY updated_at DESC, name").fetchdf()

def load_query(con: duckdb.DuckDBPyConnection, name: str) -> str:
    row = con.execute("SELECT sql FROM meta_queries WHERE name = ?", [name]).fetchone()
    if not row:
        raise ValueError("Requête introuvable")
    return row[0]

def delete_query(con: duckdb.DuckDBPyConnection, name: str) -> None:
    con.execute("DELETE FROM meta_queries WHERE name = ?", [name])

def clone_query(con: duckdb.DuckDBPyConnection, source_name: str, new_name: str) -> None:
    row = con.execute("SELECT sql, tags FROM meta_queries WHERE name = ?", [source_name]).fetchone()
    if not row:
        raise ValueError("Requête source introuvable")
    sql_text, tags = row
    save_query(con, new_name, sql_text, tags)

def export_query_catalog(con: duckdb.DuckDBPyConnection, path: str, fmt: str = "csv") -> None:
    fmt = fmt.lower()
    import json as _json
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    if fmt == "csv":
        con.execute(f"COPY (SELECT name, sql, tags, updated_at FROM meta_queries) TO '{path}' (FORMAT 'csv', HEADER true);")
    elif fmt == "json":
        df = con.execute("SELECT name, sql, COALESCE(tags, '') AS tags, CAST(updated_at AS VARCHAR) AS updated_at FROM meta_queries").fetchdf()
        with open(path, "w", encoding="utf-8") as f:
            _json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    else:
        raise ValueError("fmt doit être 'csv' ou 'json'")

def import_query_catalog(con: duckdb.DuckDBPyConnection, path: str, fmt: str = "csv", mode: str = "append") -> None:
    fmt = fmt.lower()
    if fmt == "csv":
        con.execute(f"CREATE TEMP TABLE _tmp_import AS SELECT * FROM read_csv_auto('{path}', HEADER TRUE);")
    elif fmt == "json":
        con.execute(f"CREATE TEMP TABLE _tmp_import AS SELECT name, sql, tags, updated_at FROM read_json_auto('{path}');")
    else:
        raise ValueError("fmt doit être 'csv' ou 'json'")
    if mode == "replace":
        con.execute("DELETE FROM meta_queries;")
    con.execute("DELETE FROM meta_queries WHERE name IN (SELECT name FROM _tmp_import);")
    con.execute("INSERT INTO meta_queries(name, sql, tags, created_at, updated_at) SELECT name, sql, tags, now(), COALESCE(updated_at, now()) FROM _tmp_import;")
    con.execute("DROP TABLE _tmp_import;")

# ---------------------------------------------------------------------------
# Union/Concat (auto & mapping)
# ---------------------------------------------------------------------------

def _column_names(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    df = table_schema(con, table)
    return df['name'].tolist()

def build_union_sql_by_name(
    con: duckdb.DuckDBPyConnection,
    table_a: str,
    table_b: str,
    union_all: bool = True,
    force_varchar: bool = False,
    target_table: Optional[str] = None
) -> str:
    cols_a = set(_column_names(con, table_a))
    cols_b = set(_column_names(con, table_b))
    all_cols = sorted(list(cols_a | cols_b))  # union de colonnes

    def expr(table: str, col: str) -> str:
        qi = quote_ident(col)
        base = f"{quote_ident(table)}.{qi}" if (col in (cols_a if table == table_a else cols_b)) else "NULL"
        return f"CAST({base} AS VARCHAR) AS {qi}" if force_varchar else f"{base} AS {qi}"

    sel_a = ', '.join(expr(table_a, c) for c in all_cols)
    sel_b = ', '.join(expr(table_b, c) for c in all_cols)
    op = 'UNION ALL' if union_all else 'UNION'
    create = f"CREATE OR REPLACE TABLE {quote_ident(target_table)} AS\n" if target_table else ""
    sql = f"""{create}
SELECT {sel_a} FROM {quote_ident(table_a)}
{op} BY NAME
SELECT {sel_b} FROM {quote_ident(table_b)};
"""
    return sql

def build_union_sql_with_mapping(
    con: duckdb.DuckDBPyConnection,
    table_a: str,
    table_b: str,
    mapping: Dict[str, Dict[str, Optional[str]]],
    union_all: bool = True,
    force_varchar: bool = False,
    target_table: Optional[str] = None
) -> str:
    targets = list(mapping.keys())

    def expr_side(side: str, table: str) -> str:
        parts = []
        for tgt in targets:
            src = mapping.get(tgt, {}).get(side)
            qi_tgt = quote_ident(tgt)
            src_expr = f"{quote_ident(table)}.{quote_ident(src)}" if src else "NULL"
            parts.append(f"CAST({src_expr} AS VARCHAR) AS {qi_tgt}" if force_varchar else f"{src_expr} AS {qi_tgt}")
        return ', '.join(parts)

    sel_a = expr_side('A', table_a)
    sel_b = expr_side('B', table_b)
    op = 'UNION ALL' if union_all else 'UNION'
    create = f"CREATE OR REPLACE TABLE {quote_ident(target_table)} AS\n" if target_table else ""
    sql = f"""{create}
SELECT {sel_a} FROM {quote_ident(table_a)}
{op} BY NAME
SELECT {sel_b} FROM {quote_ident(table_b)};
"""
    return sql

# ---------------------------------------------------------------------------
# Atelier: préparation & comparaison
# ---------------------------------------------------------------------------

def build_select_sql(table: str, alias_map: Dict[str, str], where_clause: Optional[str] = None) -> str:
    if not alias_map:
        raise ValueError("alias_map vide")
    parts = [f"{quote_ident(src)} AS {quote_ident(alias)}" for src, alias in alias_map.items()]
    where = f" WHERE {where_clause}" if where_clause and where_clause.strip() else ""
    return f"SELECT {', '.join(parts)} FROM {quote_ident(table)}{where}"

def materialize_select(
    con: duckdb.DuckDBPyConnection,
    target_table: str,
    table: str,
    alias_map: Dict[str, str],
    where_clause: Optional[str] = None
) -> None:
    sql = build_select_sql(table, alias_map, where_clause)
    con.execute(f"CREATE OR REPLACE TABLE {quote_ident(target_table)} AS {sql}")

def compare_prepared(con: duckdb.DuckDBPyConnection, prep_a: str, prep_b: str) -> Dict[str, int]:
    A = quote_ident(prep_a)
    B = quote_ident(prep_b)
    total_a = con.execute(f"SELECT COUNT(*) FROM {A}").fetchone()[0]
    total_b = con.execute(f"SELECT COUNT(*) FROM {B}").fetchone()[0]
    diff_ab = con.execute(f"SELECT COUNT(*) FROM (SELECT * FROM {A} EXCEPT SELECT * FROM {B}) t").fetchone()[0]
    diff_ba = con.execute(f"SELECT COUNT(*) FROM (SELECT * FROM {B} EXCEPT SELECT * FROM {A}) t").fetchone()[0]
    equal_sets = (diff_ab == 0 and diff_ba == 0 and total_a == total_b)
    return {
        'rows_a': int(total_a),
        'rows_b': int(total_b),
        'a_minus_b': int(diff_ab),
        'b_minus_a': int(diff_ba),
        'equal_sets': int(1 if equal_sets else 0)
    }

# ---------------------------------------------------------------------------
# SQLAlchemy & export (avec coercions et pré-count)
# ---------------------------------------------------------------------------

def connect_external_sqlalchemy(url: str):
    return create_engine(url, pool_pre_ping=True, future=True)

def ingest_external_df(
    con: duckdb.DuckDBPyConnection,
    engine,
    sql_or_table: str,
    table_name: str,
    is_table: bool = False,
    mode: str = 'replace',
    chunksize: Optional[int] = None,
    progress_cb = None,  # callback: (rows_total:int|None, rows_chunk:int, chunks_done:int) -> None
    pre_count: bool = False,
    count_query: Optional[str] = None,
    coerce_str_cols: Optional[List[str]] = None,
    coerce_bigint_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Ingestion générique (SQLAlchemy) avec support:
      - des chunks
      - du pré-comptage COUNT(*) pour une progression précise
      - des coercions de colonnes (ex. siret → string, `coerce_str_cols`)
    Si chunksize est None: lecture complète (retourne le DataFrame complet).
    Sinon: itère par chunks (retourne un aperçu DuckDB).
    """
    logger = logging.getLogger('duckapp')
    read_fn = (pd.read_sql_table if is_table else pd.read_sql_query)

    # -- Pré-count (facultatif)
    total = None
    if pre_count:
        try:
            if count_query:
                q = count_query
            elif is_table:
                q = f"SELECT COUNT(*) AS n FROM {sql_or_table}"
            else:
                q = f"SELECT COUNT(*) AS n FROM ({sql_or_table}) t"
            with engine.connect() as conn:
                total = conn.execute(text(q)).scalar()
            logger.info("Pré-comptage source: total=%s", total)
            if progress_cb:
                progress_cb(total, 0, 0)
        except Exception as e:
            logger.warning("Pré-comptage impossible (%s). On continue sans.", e)

    # -- Mode FULL (pas de chunks)
    if chunksize is None:
        logger.info("Ingestion full (no chunks) depuis source externe; coercions str=%s, bigint=%s",
                    coerce_str_cols, coerce_bigint_cols)
        df = read_fn(sql_or_table, con=engine)
        _coerce_cols_to_string(df, coerce_str_cols)
        _coerce_cols_to_bigint(df, coerce_bigint_cols)

        persist_df(con, table_name, df, mode=mode)
        if progress_cb:
            progress_cb(total, len(df), 1)
        # Comparaison post-ingestion si on a un total
        try:
            target_count = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
            logger.info("Comparaison COUNT: source=%s, duckdb=%s, delta=%s", total, target_count,
                        (None if total is None else int(target_count) - int(total)))
        except Exception:
            pass
        logger.info("Ingestion terminée: table=%s, rows=%d, cols=%d", table_name, len(df), len(df.columns))
        return df

    # -- Mode CHUNKS
    logger.info("Ingestion par chunks: chunksize=%d, table cible=%s, mode=%s, str_cols=%s, bigint_cols=%s",
                chunksize, table_name, mode, coerce_str_cols, coerce_bigint_cols)
    chunks_done = 0
    total_rows = 0
    first = True
    for chunk in read_fn(sql_or_table, con=engine, chunksize=chunksize):
        _coerce_cols_to_string(chunk, coerce_str_cols)
        _coerce_cols_to_bigint(chunk, coerce_bigint_cols)

        rows_chunk = len(chunk)
        if first:
            persist_df(con, table_name, chunk, mode=mode)  # replace/create
            first = False
        else:
            persist_df(con, table_name, chunk, mode="append")
        chunks_done += 1
        total_rows += rows_chunk
        logger.info("Chunk %d ingéré (rows=%d, cumul=%d)", chunks_done, rows_chunk, total_rows)
        if progress_cb:
            progress_cb(total, rows_chunk, chunks_done)

    # Aperçu + comparaison
    df_head = con.execute(f"SELECT * FROM {quote_ident(table_name)} LIMIT 200").fetchdf()
    try:
        target_count = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
        logger.info("Comparaison COUNT: source=%s, duckdb=%s, delta=%s", total, target_count,
                    (None if total is None else int(target_count) - int(total)))
    except Exception:
        pass
    logger.info("Ingestion par chunks terminée: total_rows~=%d, chunks=%d", total_rows, chunks_done)
    return df_head

def export_table(con: duckdb.DuckDBPyConnection, table: str, path: str, fmt: str = 'parquet', header: bool = True) -> None:
    table_q = quote_ident(table)
    fmt = fmt.lower()
    if fmt == 'parquet':
        con.execute(f"COPY (SELECT * FROM {table_q}) TO '{path}' (FORMAT 'parquet');")
    elif fmt == 'csv':
        header_opt = 'HEADER true' if header else 'HEADER false'
        con.execute(f"COPY (SELECT * FROM {table_q}) TO '{path}' (FORMAT 'csv', {header_opt});")
    else:
        raise ValueError("fmt doit être 'parquet' ou 'csv'")
