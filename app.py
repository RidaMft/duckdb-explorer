
"""
DuckDB SQL Explorer + Snowflake (SSO)
- Barre d'état + barre de progression
- Ingestion CSV/SQLAlchemy (chunks) + logs
- Comparer / Profilage / Schéma
- Nouvel onglet : Snowflake (SSO) avec profils enregistrés + transferts DuckDB↔Snowflake
"""
import traceback
import os
import json
import logging
from typing import List

import pandas as pd
import streamlit as st
import duckdb

from backend import (
    make_duckdb_connection,
    list_tables,
    table_schema,
    table_rowcount,
    profile_table,
    enable_extensions,
    attach_external,
    export_table,
    quote_ident,
    materialize_select,
    build_select_sql,
    compare_prepared,
    validate_select_only,
    run_query,
    profile_query,
    explain_analyze,
    init_query_catalog,
    save_query,
    list_queries,
    load_query,
    delete_query,
    clone_query,
    export_query_catalog,
    import_query_catalog,
    connect_external_sqlalchemy,
    ingest_external_df,
    load_csv_to_df,
    load_csv_to_duckdb_stream,
    persist_df,
    init_logging,
)

from snowflake_tools import (
    sf_connect_externalbrowser,
    sf_run_query,
    sf_list_tables,
    sf_copy_duckdb_to_snowflake,
    sf_copy_snowflake_to_duckdb,
    sf_normalize_account,
    sf_current_context,
    # persistence (JSON)
    load_state,
    save_state,
    list_sf_profiles,
    save_sf_profile,
    delete_sf_profile,
    list_sqlalchemy_connections,
    save_sqlalchemy_connection,
    delete_sqlalchemy_connection,
)

# ----------------------
# Config & Keys
# ----------------------
st.set_page_config(page_title="DuckDB SQL Explorer", page_icon="🦆", layout="wide")
ss = st.session_state
KEY_PREFIX = {
    'cmp': 'cmp', 'sql': 'sql', 'cat': 'cat', 'csv': 'csv', 'src': 'src',
    'sch': 'sch', 'pro': 'pro', 'ext': 'ext', 'att': 'att', 'sf': 'sf'
}

def k(page: str, name: str) -> str:
    return f"{KEY_PREFIX.get(page, page)}_{name}"

# ----------------------
# Logging
# ----------------------
if 'logger_initialized' not in ss:
    os.makedirs('logs', exist_ok=True)
    init_logging(log_dir='logs', level=logging.INFO)
    ss.logger_initialized = True
logger = logging.getLogger('duckapp')

# ----------------------
# Helper UI : status + progress
# ----------------------
from contextlib import contextmanager
class _Progress:
    def __init__(self, title, st_status, prog, logger):
        self.title = title
        self._st_status = st_status
        self._prog = prog
        self._logger = logger
    def tick(self, ratio: float | None = None, msg: str | None = None):
        if msg:
            self._st_status.update(label=f"{self.title} • {msg}")
            self._logger.info("[UI] %s", msg)
        if ratio is not None:
            self._prog.progress(min(max(ratio, 0.0), 1.0))

@contextmanager
def progress_status(title: str = "Traitement en cours...", expanded: bool = True):
    st_status = st.status(label=title, expanded=expanded)
    prog = st.progress(0.0)
    ps = _Progress(title, st_status, prog, logger)
    try:
        yield ps
        st_status.update(label=f"{title} • Terminé", state="complete")
        prog.progress(1.0)
    except Exception as e:
        st_status.update(label=f"{title} • Erreur: {e}", state="error")
        raise

# ----------------------
# DuckDB connection & state
# ----------------------
if 'db_path' not in ss:
    ss.db_path = 'data/catalog.duckdb'
if 'duck_con' not in ss:
    ss.duck_con = make_duckdb_connection(ss.db_path)
    logger.info("Connecté à DuckDB: %s", ss.db_path)
try:
    init_query_catalog(ss.duck_con)
except Exception:
    pass
if 'external_eng' not in ss:
    ss.external_eng = None
if 'csv_preview_df' not in ss:
    ss.csv_preview_df = None
    ss.csv_preview_meta = None

# ----------------------
# Sidebar
# ----------------------
with st.sidebar:
    st.subheader("Backend DuckDB")
    new_path = st.text_input("Chemin base", value=ss.db_path, key=k('src','db_path'))
    if st.button("(Re)connecter", key=k('src','reconnect')):
        try:
            ss.duck_con = make_duckdb_connection(new_path)
            ss.db_path = new_path
            init_query_catalog(ss.duck_con)
            st.success(f"Connecté à DuckDB ({new_path}).")
            logger.info("Reconnecté à DuckDB: %s", new_path)
        except Exception as e:
            st.error(f"Erreur connexion: {e}")
            st.code(traceback.format_exc())
            logger.exception("Erreur de reconnexion à DuckDB")
    if st.button("Activer extensions (postgres/mysql/sqlite/httpfs/json)", key=k('att','enable_ext')):
        try:
            enable_extensions(ss.duck_con, install=True)
            st.success("Extensions activées.")
            logger.info("Extensions DuckDB activées")
        except Exception as e:
            st.error(f"Erreur activation extensions: {e}")
            st.info("Astuce: si l’installation échoue, essayez `INSTALL ... FROM core`. ")
            st.code(traceback.format_exc())
            logger.exception("Erreur d'activation des extensions")

# ----------------------
# Title & Tabs
# ----------------------
st.title("🦆 DuckDB SQL Explorer")
st.caption("Comparer, requêter, profiler et gérer vos données DuckDB & Snowflake en toute simplicité.")
TAB_ORDER = [
    "SQL", "Comparer", "Catalogue", "Importer CSV", "Sources",
    "Schéma", "Profilage", "Source externe → DuckDB", "Extensions ATTACH",
    "Snowflake (SSO)"
]
_TABS = st.tabs(TAB_ORDER)
TAB = {name: tab for name, tab in zip(TAB_ORDER, _TABS)}

# ----------------------
# Small helpers
# ----------------------
def _download_button_csv(df: pd.DataFrame, filename: str, label: str):
    st.download_button(label=label, data=df.to_csv(index=False).encode('utf-8'), file_name=filename, mime='text/csv')

# ----------------------
# Pages
# ----------------------

def render_sql_page():
    with TAB["SQL"]:
        st.subheader("SQL Workbench (SELECT/CTE uniquement)")
        placeholder_sql = """-- Exemples
SELECT * FROM table1 LIMIT 100;
-- Jointure cross-sources si attachées:
-- SELECT a.* FROM tableA a LEFT JOIN tableB b USING (id);"""
        if 'sql_editor' not in ss:
            ss.sql_editor = placeholder_sql
        st.text_area("Votre requête SQL", height=220, key=k('sql','editor'))
        c1, c2, c3 = st.columns(3)
        with c1:
            page_size = st.number_input("Taille de page", min_value=50, max_value=10000, value=200, step=50, key=k('sql','page_size'))
        with c2:
            page = st.number_input("Page", min_value=1, value=1, step=1, key=k('sql','page'))
        with c3:
            run_btn = st.button("Exécuter", key=k('sql','run'))
        if run_btn:
            sql_query = ss.sql_editor
            ok, msg = validate_select_only(sql_query)
            if not ok:
                st.error(msg)
                logger.warning("Validation SQL échouée: %s", msg)
            else:
                try:
                    elapsed, full_df, exp_df = profile_query(ss.duck_con, sql_query)
                    start = max(0, (page - 1) * page_size)
                    end = start + page_size
                    page_df = full_df.iloc[start:end]
                    st.success(f"Résultat: {len(full_df):,} lignes (affichées: {len(page_df):,}) • Temps: {elapsed:.3f}s")
                    tabs = st.tabs(["Résultats", "Profilage (EXPLAIN ANALYZE)", "SQL"])
                    with tabs[0]:
                        st.dataframe(page_df, use_container_width=True)
                        _download_button_csv(full_df, "resultat_sql.csv", "📥 Télécharger (CSV)")
                    with tabs[1]:
                        if exp_df is not None and not exp_df.empty:
                            st.dataframe(exp_df, use_container_width=True)
                            _download_button_csv(exp_df, "explain_analyze.csv", "📥 Télécharger le profil (CSV)")
                        else:
                            st.info("EXPLAIN ANALYZE non disponible.")
                    with tabs[2]:
                        st.code(sql_query, language="sql")
                    logger.info("Requête SQL exécutée (%d lignes, %.3fs)", len(full_df), elapsed)
                except Exception as e:
                    st.error(f"Erreur d'exécution: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur exécution requête SQL")
        st.divider()
        st.markdown("**Sauvegarde rapide dans le catalogue**")
        qname = st.text_input("Nom de la requête", value="ma_requete", key=k('sql','qname'))
        qtags = st.text_input("Tags (optionnel)", value="ad-hoc, demo", key=k('sql','qtags'))
        if st.button("💾 Sauvegarder la requête", key=k('sql','save')):
            try:
                save_query(ss.duck_con, qname, ss.sql_editor, qtags)
                st.success(f"Requête '{qname}' sauvegardée.")
                logger.info("Requête sauvegardée: %s", qname)
            except Exception as e:
                st.error(f"Erreur sauvegarde: {e}")
                logger.exception("Erreur sauvegarde requête")

def render_catalog_page():
    with TAB["Catalogue"]:
        st.subheader("Catalogue de requêtes (DuckDB)")
        try:
            qdf = list_queries(ss.duck_con)
            search = st.text_input("Filtrer par nom/tag", key=k('cat','search'))
            if search:
                qdf = qdf[qdf.apply(lambda r: search.lower() in (str(r['name'])+str(r['tags'])).lower(), axis=1)]
            if qdf.empty:
                st.info("Aucune requête sauvegardée.")
            else:
                st.dataframe(qdf, use_container_width=True)
                sel = st.selectbox("Sélectionner une requête", qdf['name'].tolist() if not qdf.empty else [], key=k('cat','select'))
                colx, coly, colz, colw = st.columns(4)
                with colx:
                    if st.button("📥 Charger dans l'éditeur", key=k('cat','load')) and sel:
                        try:
                            sql_text = load_query(ss.duck_con, sel)
                            ss.sql_editor = sql_text
                            st.success(f"Requête '{sel}' chargée dans l'éditeur (onglet SQL).")
                            logger.info("Requête chargée: %s", sel)
                        except Exception as e:
                            st.error(f"Erreur chargement: {e}")
                with coly:
                    if st.button("📋 Copier", key=k('cat','copy')) and sel:
                        try:
                            sql_text = load_query(ss.duck_con, sel)
                            st.code(sql_text, language="sql")
                            st.caption("Clique sur l’icône copie dans le bloc ci-dessus.")
                            logger.info("Requête copiée (affichée): %s", sel)
                        except Exception as e:
                            st.error(f"Erreur copie: {e}")
                with colz:
                    new_name = st.text_input("Nom de la copie (clone)", value=(f"{sel}_copy" if sel else "new_query"), key=k('cat','clone_name'))
                    if st.button("🧬 Dupliquer", key=k('cat','clone_btn')) and sel and new_name:
                        try:
                            clone_query(ss.duck_con, sel, new_name)
                            st.success(f"Copie créée: '{new_name}'.")
                            logger.info("Requête clonée: %s -> %s", sel, new_name)
                        except Exception as e:
                            st.error(f"Erreur duplication: {e}")
                with colw:
                    if st.button("▶️ Exécuter", key=k('cat','exec')) and sel:
                        try:
                            sql_text = load_query(ss.duck_con, sel)
                            elapsed, result_df, exp_df = profile_query(ss.duck_con, sql_text)
                            st.success(f"Exécutée: {len(result_df):,} lignes • {elapsed:.3f}s")
                            st.dataframe(result_df.head(200), use_container_width=True)
                            _download_button_csv(result_df, f"resultat_{sel}.csv", "📥 Export résultat (CSV)")
                            logger.info("Requête exécutée depuis catalogue: %s", sel)
                        except Exception as e:
                            st.error(f"Erreur exécution: {e}")
                            st.code(traceback.format_exc())
        except Exception as e:
            st.error(f"Erreur catalogue: {e}")
            st.code(traceback.format_exc())
            logger.exception("Erreur catalogue")

def render_import_csv_page():
    with TAB["Importer CSV"]:
        st.subheader("Importer un fichier CSV → table DuckDB")
        st.caption("Séparateurs multi-caractères, encodages variés, **progression + logs**. Mode *streamé* pour gros fichiers. Utilisez *Colonnes à forcer en texte* pour SIRET/SIREN.")
        uploaded = st.file_uploader("Sélectionner un fichier CSV", type=["csv"], key=k('csv','uploader'))
        col_l, col_r = st.columns(2)
        with col_l:
            sep = st.text_input("Séparateur", value=",", key=k('csv','sep'))
            encoding = st.text_input("Encodage", value="utf-8", key=k('csv','encoding'))
            header_row = st.number_input("Index ligne d’en-tête (0 = première)", min_value=-1, value=0, step=1, key=k('csv','header'))
        with col_r:
            quotechar = st.text_input("Quote char (vide = désactivé)", value='"', key=k('csv','quote'))
            escapechar = st.text_input("Escape char (vide = désactivé)", value="", key=k('csv','escape'))
            parse_mode = st.selectbox("Mode parsing séc. multi-caractères", options=["literal", "regex"], index=0, key=k('csv','parse_mode'))
        st.markdown('---')
        with st.expander('Options avancées CSV', expanded=False):
            low_memory = st.checkbox('low_memory (optimiser mémoire)', value=False, help='Peut générer des DtypeWarning si colonnes mixtes.', key=k('csv','low_memory'))
            dtype_json = st.text_area('dtype (JSON facultatif)', value='', help='Ex: {"col1": "string", "col2": "Int64"}', key=k('csv','dtype_json'))
            try:
                dtype_map = json.loads(dtype_json) if dtype_json.strip() else None
            except Exception:
                st.warning('dtype JSON invalide — ignoré')
                dtype_map = None
        target_table = st.text_input("Nom de la table cible", value="import_csv", key=k('csv','target'))
        mode = st.selectbox("Mode d'écriture", options=["replace","append","create"], index=0, key=k('csv','mode'))
        str_cols_csv = st.text_input("Colonnes à forcer en texte (CSV)", value="siret", key=k('csv','str_cols'))
        str_cols_list = [c.strip() for c in str_cols_csv.split(',') if c.strip()]

        if st.button("📂 Charger le fichier CSV", key=k('csv','load_btn')):
            if uploaded is None:
                st.warning("Aucun fichier sélectionné.")
            else:
                try:
                    with progress_status("Lecture CSV (preview)") as ps:
                        ps.tick(0.1, "Décodage & parsing…")
                        df = load_csv_to_df(
                            uploaded,
                            sep=sep if sep else ",",
                            encoding=encoding if encoding else "utf-8",
                            header=(None if header_row < 0 else int(header_row)),
                            quotechar=(None if (quotechar or '').strip()=="" else quotechar),
                            escapechar=(None if (escapechar or '').strip()=="" else escapechar),
                            parse_mode=parse_mode,
                            low_memory=low_memory,
                            dtype=dtype_map,
                        )
                        from backend import _coerce_cols_to_string as _to_str
                        _to_str(df, str_cols_list)
                        ps.tick(0.8, f"Préparation de l’aperçu… {len(df):,} lignes")
                        ss.csv_preview_df = df
                        ss.csv_preview_meta = {
                            'rows': len(df), 'cols': len(df.columns),
                            'sep': sep, 'encoding': encoding, 'header': header_row,
                            'parse_mode': parse_mode, 'low_memory': low_memory
                        }
                        st.success(f"Fichier chargé : {len(df):,} lignes, {len(df.columns)} colonnes")
                        st.dataframe(df.head(200), use_container_width=True)
                        logger.info("CSV CHARGÉ (%d lignes, %d colonnes)", len(df), len(df.columns))
                except Exception as e:
                    st.error(f"Erreur de lecture CSV: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur lecture CSV")

        stream_mode = st.checkbox("Import streamé (gros CSV, faible mémoire)", value=False, key=k('csv','stream'))
        chunksize = st.number_input("Chunk size CSV (stream)", min_value=10_000, value=200_000, step=50_000, key=k('csv','chunksize'))
        if st.button("Importer → DuckDB", key=k('csv','import_btn')):
            if ss.csv_preview_df is None and not stream_mode:
                st.warning("Charge d'abord le fichier CSV (bouton ci-dessus).")
            else:
                try:
                    if not stream_mode:
                        with progress_status("Import CSV → DuckDB") as ps:
                            ps.tick(0.2, "Écriture dans DuckDB…")
                            persist_df(ss.duck_con, target_table, ss.csv_preview_df, mode=mode)
                            ps.tick(0.95, "Finalisation…")
                            st.success(f"Import terminé : table '{target_table}' ({ss.csv_preview_meta['rows']:,} lignes).")
                            st.caption("Vérifiez la table dans l’onglet 'Sources'.")
                            logger.info("CSV importé: table=%s (mode=%s)", target_table, mode)
                    else:
                        with progress_status("Import CSV streamé → DuckDB") as ps:
                            processed = {"rows": 0, "chunks": 0}
                            def cb(total, rows_chunk, chunks_done):
                                processed["rows"] += rows_chunk
                                processed["chunks"] = chunks_done
                                ratio = min(0.98, processed["rows"]/total) if total else min(0.98, 0.05 + 0.15*chunks_done)
                                ps.tick(ratio, f"Chunk {chunks_done} (+{rows_chunk:,}) • total {processed['rows']:,}")
                            uploaded.seek(0)
                            df_head = load_csv_to_duckdb_stream(
                                ss.duck_con, uploaded, target_table,
                                sep=sep, encoding=encoding, header=header_row,
                                quotechar=(None if (quotechar or '').strip()=="" else quotechar),
                                escapechar=(None if (escapechar or '').strip()=="" else escapechar),
                                parse_mode=parse_mode,
                                chunksize=int(chunksize),
                                progress_cb=cb,
                                mode=mode,
                                coerce_str_cols=str_cols_list,
                                coerce_bigint_cols=None,
                            )
                            ps.tick(0.99, "Aperçu")
                            st.success(f"Import streamé terminé : '{target_table}' • ~{processed['rows']:,} lignes (chunks={processed['chunks']})")
                            st.dataframe(df_head, use_container_width=True)
                            logger.info("CSV streamé importé: table=%s, rows~=%d, chunks=%d", target_table, processed['rows'], processed['chunks'])
                except Exception as e:
                    st.error(f"Erreur d’import: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur import CSV dans DuckDB")

def render_sources_page():
    with TAB["Sources"]:
        st.subheader("Tables DuckDB")
        try:
            tbl_df = list_tables(ss.duck_con)
            if tbl_df.empty:
                st.info("Aucune table.")
            else:
                tbl_df['rows'] = [table_rowcount(ss.duck_con, t) for t in tbl_df['table_name']]
                st.dataframe(tbl_df, use_container_width=True)
                to_drop = st.multiselect("Supprimer des tables", tbl_df['table_name'].tolist() if not tbl_df.empty else [], key=k('src','drop'))
                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("Supprimer sélection", key=k('src','drop_btn')) and to_drop:
                        for t in to_drop:
                            try:
                                ss.duck_con.execute(f"DROP TABLE IF EXISTS {quote_ident(t)}")
                                logger.info("Table supprimée: %s", t)
                            except Exception as e:
                                st.error(f"DROP {t}: {e}")
                                logger.exception("Erreur DROP table: %s", t)
                        st.success("Tables supprimées.")
                with c2:
                    st.markdown("**Exporter une table**")
                    exp_table = st.selectbox("Table à exporter", tbl_df['table_name'].tolist() if not tbl_df.empty else [], key=k('src','exp_table'))
                    fmt = st.selectbox("Format", ["parquet","csv"], key=k('src','fmt'))
                    out_dir = st.text_input("Dossier de sortie", value="exports", key=k('src','out_dir'))
                    out_name = st.text_input("Nom de fichier", value=(f"{exp_table}.{fmt}" if exp_table else "export.csv"), key=k('src','out_name'))
                    if st.button("Exporter", key=k('src','export_btn')) and exp_table:
                        try:
                            os.makedirs(out_dir, exist_ok=True)
                            path = os.path.join(out_dir, out_name)
                            export_table(ss.duck_con, exp_table, path, fmt=fmt, header=True)
                            st.success(f"Exporté: {path}")
                            logger.info("Table exportée: %s -> %s (%s)", exp_table, path, fmt)
                        except Exception as e:
                            st.error(f"Erreur export: {e}")
                            st.code(traceback.format_exc())
                            logger.exception("Erreur export table")
                with c3:
                    if st.button("VACUUM (compactage)", key=k('src','vacuum')):
                        try:
                            ss.duck_con.execute("VACUUM")
                            st.success("Base compactée.")
                            logger.info("VACUUM exécuté")
                        except Exception as e:
                            st.error(f"Erreur VACUUM: {e}")
                            logger.exception("Erreur VACUUM")
        except Exception as e:
            st.error(f"Erreur listage tables: {e}")
            st.code(traceback.format_exc())
            logger.exception("Erreur listage tables")

def render_schema_page():
    with TAB["Schéma"]:
        st.subheader("Schéma de table")
        try:
            tables = list_tables(ss.duck_con)['table_name'].tolist()
        except Exception:
            tables = []
        if not tables:
            st.info("Aucune table.")
        else:
            table = st.selectbox("Table", tables, key=k('sch','table'))
            if table:
                try:
                    schema_df = table_schema(ss.duck_con, table)
                    st.dataframe(schema_df, use_container_width=True)
                    st.caption(f"Lignes: {table_rowcount(ss.duck_con, table):,}")
                except Exception as e:
                    st.error(f"Erreur schéma: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur schéma table: %s", table)

def render_comparer_page():
    with TAB["Comparer"]:
        st.subheader("Comparer deux tables préparées")
        try:
            tables_df = list_tables(ss.duck_con)
            all_tables = tables_df['table_name'].tolist()
        except Exception:
            all_tables = []
        cA, cB = st.columns(2)
        with cA:
            t_a = st.selectbox("Source A", all_tables, key=k('cmp','source_a'))
        with cB:
            t_b = st.selectbox("Source B", [t for t in all_tables if t != t_a], key=k('cmp','source_b'))
        if t_a and t_b:
            cols_a = table_schema(ss.duck_con, t_a)['name'].tolist()
            cols_b = table_schema(ss.duck_con, t_b)['name'].tolist()
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**A — Colonnes & alias**")
                sel_a = st.multiselect("Colonnes A", cols_a, default=cols_a[:min(5,len(cols_a))], key=k('cmp','sel_a'))
                alias_map_a = {c: st.text_input(f"Alias pour A.{c}", value=c, key=k('cmp',f'alias_a_{c}')) for c in sel_a}
                where_a = st.text_input("Clause WHERE (A)", value="", key=k('cmp','where_a'))
            with c2:
                st.markdown("**B — Colonnes & alias**")
                sel_b = st.multiselect("Colonnes B", cols_b, default=cols_b[:min(5,len(cols_b))], key=k('cmp','sel_b'))
                alias_map_b = {c: st.text_input(f"Alias pour B.{c}", value=c, key=k('cmp',f'alias_b_{c}')) for c in sel_b}
                where_b = st.text_input("Clause WHERE (B)", value="", key=k('cmp','where_b'))
            st.caption("Aperçus des SELECT générés")
            try:
                st.code(build_select_sql(t_a, alias_map_a, where_a), language='sql')
                st.code(build_select_sql(t_b, alias_map_b, where_b), language='sql')
            except Exception as e:
                st.error(f"Erreur SELECT: {e}")
            prep_a = st.text_input("Nom du dataset préparé A", value="prep_A", key=k('cmp','prep_a'))
            prep_b = st.text_input("Nom du dataset préparé B", value="prep_B", key=k('cmp','prep_b'))
            c3, c4 = st.columns(2)
            with c3:
                if st.button("Créer/Remplacer A", key=k('cmp','build_a')):
                    try:
                        materialize_select(ss.duck_con, prep_a, t_a, alias_map_a, where_a)
                        st.success(f"Dataset '{prep_a}' créé.")
                        df_prev = ss.duck_con.execute(f"SELECT * FROM {quote_ident(prep_a)} LIMIT 50").fetchdf()
                        st.dataframe(df_prev, use_container_width=True)
                        logger.info("Dataset préparé A créé: %s", prep_a)
                    except Exception as e:
                        st.error(f"Erreur préparation A: {e}")
                        st.code(traceback.format_exc())
                        logger.exception("Erreur préparation A")
            with c4:
                if st.button("Créer/Remplacer B", key=k('cmp','build_b')):
                    try:
                        materialize_select(ss.duck_con, prep_b, t_b, alias_map_b, where_b)
                        st.success(f"Dataset '{prep_b}' créé.")
                        df_prev = ss.duck_con.execute(f"SELECT * FROM {quote_ident(prep_b)} LIMIT 50").fetchdf()
                        st.dataframe(df_prev, use_container_width=True)
                        logger.info("Dataset préparé B créé: %s", prep_b)
                    except Exception as e:
                        st.error(f"Erreur préparation B: {e}")
                        st.code(traceback.format_exc())
                        logger.exception("Erreur préparation B")
            st.markdown("**Comparer la volumétrie & l'égalité (sets)**")
            if st.button("Comparer A vs B", key=k('cmp','compare')):
                try:
                    m = compare_prepared(ss.duck_con, prep_a, prep_b)
                    colx, coly = st.columns(2)
                    with colx:
                        st.metric(label="Lignes A", value=f"{m['rows_a']:,}")
                        st.metric(label="A \\ B", value=f"{m['a_minus_b']:,}")
                    with coly:
                        st.metric(label="Lignes B", value=f"{m['rows_b']:,}")
                        st.metric(label="B \\ A", value=f"{m['b_minus_a']:,}")
                    st.success("Jeux équivalents" if m['equal_sets'] else "Jeux différents")
                    try:
                        cols_pa = table_schema(ss.duck_con, prep_a)['name'].tolist()
                        cols_pb = table_schema(ss.duck_con, prep_b)['name'].tolist()
                        common_cols = [c for c in cols_pa if c in cols_pb]
                        if common_cols:
                            cols_sql = ", ".join([quote_ident(c) for c in common_cols])
                            sql_ab = f"SELECT {cols_sql} FROM {quote_ident(prep_a)} EXCEPT SELECT {cols_sql} FROM {quote_ident(prep_b)} LIMIT 100"
                            sql_ba = f"SELECT {cols_sql} FROM {quote_ident(prep_b)} EXCEPT SELECT {cols_sql} FROM {quote_ident(prep_a)} LIMIT 100"
                            df_ab = ss.duck_con.execute(sql_ab).fetchdf()
                            df_ba = ss.duck_con.execute(sql_ba).fetchdf()
                        else:
                            df_ab = pd.DataFrame(columns=['(aucune colonne commune)'])
                            df_ba = pd.DataFrame(columns=['(aucune colonne commune)'])
                        t1, t2 = st.tabs(["A \\ B", "B \\ A"])
                        with t1:
                            st.dataframe(df_ab, use_container_width=True)
                            _download_button_csv(df_ab, "diff_A_minus_B.csv", "📥 Exporter (CSV)")
                        with t2:
                            st.dataframe(df_ba, use_container_width=True)
                            _download_button_csv(df_ba, "diff_B_minus_A.csv", "📥 Exporter (CSV)")
                        logger.info("Comparaison A vs B effectuée: %s vs %s", prep_a, prep_b)
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Erreur comparaison: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur comparaison A vs B")

def render_profile_page():
    with TAB["Profilage"]:
        st.subheader("Profilage de table")
        try:
            tables = list_tables(ss.duck_con)['table_name'].tolist()
        except Exception:
            tables = []
        if not tables:
            st.info("Aucune table.")
        else:
            table = st.selectbox("Table à profiler", tables, key=k('pro','table'))
            top_n = st.number_input("Top-N pour catégorielles", min_value=1, max_value=50, value=5, key=k('pro','top_n'))
            if st.button("Profiler", key=k('pro','run')) and table:
                try:
                    prof = profile_table(ss.duck_con, table, top_n=int(top_n))
                    sub_tabs = st.tabs(["Schéma", "Numériques", "Catégorielles", "NULLs"])
                    with sub_tabs[0]:
                        st.dataframe(prof['schema_df'], use_container_width=True)
                    with sub_tabs[1]:
                        if not prof['numeric_stats_df'].empty:
                            st.dataframe(prof['numeric_stats_df'], use_container_width=True)
                        else:
                            st.info("Aucune colonne numérique.")
                    with sub_tabs[2]:
                        if not prof['categorical_stats_df'].empty:
                            st.dataframe(prof['categorical_stats_df'], use_container_width=True)
                        else:
                            st.info("Aucune colonne catégorielle.")
                    with sub_tabs[3]:
                        st.dataframe(prof['nulls_df'], use_container_width=True)
                    logger.info("Profilage effectué sur table: %s", table)
                except Exception as e:
                    st.error(f"Erreur profilage: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur profilage table: %s", table)

def render_external_page():
    with TAB["Source externe → DuckDB"]:
        st.subheader("Connexion source externe (SQLAlchemy) puis ingestion dans DuckDB")
        st.caption(
            "Exemples d'URL : "
            "postgresql+psycopg2://user:pass@host:5432/dbname, "
            "mysql+pymysql://user:pass@host:3306/dbname, "
            "mssql+pymssql://user:pass@host:1433/dbname (ou mssql+pytds://...)"
        )
        sa_url = st.text_input("SQLAlchemy URL", value="", key=k('ext','url'))

        # Connexions enregistrées (SQLAlchemy)
        state = load_state()
        saved_conns = list_sqlalchemy_connections(state)
        names = list(saved_conns.keys())
        with st.expander("Connexions enregistrées (SQLAlchemy)", expanded=False):
            left, right = st.columns([2,2])
            with left:
                sel = st.selectbox("Choisir", names or ["(aucune)"])
                if names and st.button("Charger dans le champ", key=k('ext','load_conn')):
                    st.session_state[k('ext','url')] = saved_conns[sel]
                    st.success(f"Connexion '{sel}' chargée.")
            with right:
                new_name = st.text_input("Nom à enregistrer", value="default_conn", key=k('ext','conn_name'))
                if st.button("Enregistrer", key=k('ext','save_conn')):
                    save_sqlalchemy_connection(state, new_name, st.session_state.get(k('ext','url'), ''))
                    save_state(state)
                    st.success(f"Connexion '{new_name}' enregistrée.")
                if names and st.button("Supprimer la sélection", key=k('ext','del_conn')):
                    delete_sqlalchemy_connection(state, sel)
                    save_state(state)
                    st.success(f"Connexion '{sel}' supprimée.")

        colx, coly = st.columns(2)
        with colx:
            if st.button("Se connecter (SQLAlchemy)", key=k('ext','connect')) and sa_url:
                try:
                    ss.external_eng = connect_external_sqlalchemy(sa_url)
                    st.success("Connecté via SQLAlchemy.")
                    logger.info("Connexion SQLAlchemy établie: %s", sa_url)
                except Exception as e:
                    st.error(f"Erreur connexion: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur connexion SQLAlchemy")
        with coly:
            if st.button("Déconnecter (SQLAlchemy)", key=k('ext','disconnect')):
                ss.external_eng = None
                st.info("Déconnecté.")
                logger.info("Déconnecté de SQLAlchemy")

        if ss.external_eng is None:
            st.info("Configurez et connectez une source externe ci-dessus.")
        else:
            st.divider()
            st.subheader("Ingestion")
            sql_or_table = st.text_area("Table ou requête (SELECT)", height=160, value="SELECT 1 AS x", key=k('ext','input'))
            is_table = st.checkbox("C'est un nom de table", value=False, key=k('ext','is_table'))
            target_table = st.text_input("Nom de la table DuckDB à créer/remplacer", value="external_data", key=k('ext','target'))
            mode = st.selectbox("Mode", options=["replace","append","create"], index=0, key=k('ext','mode'))
            chunksize = st.number_input("Chunk size (0 = full load)", min_value=0, value=200_000, step=50_000, key=k('ext','chunksize'))
            do_precount = st.checkbox("Pré-calculer COUNT(*) (progression précise)", value=True, key=k('ext','precount'))
            str_cols_ext = st.text_input("Colonnes à forcer en texte (source externe)", value="siret", key=k('ext','str_cols'))
            bigint_cols_ext = st.text_input("Colonnes à forcer en BIGINT (optionnel)", value="", key=k('ext','bigint_cols'))
            str_cols_list = [c.strip() for c in str_cols_ext.split(',') if c.strip()]
            bigint_cols_list = [c.strip() for c in bigint_cols_ext.split(',') if c.strip()]
            if st.button("Ingestion → DuckDB", key=k('ext','ingest')):
                try:
                    prior_count = None
                    if mode == "append":
                        try:
                            prior_count = ss.duck_con.execute(f"SELECT COUNT(*) FROM {quote_ident(target_table)}").fetchone()[0]
                        except Exception:
                            prior_count = None
                    with progress_status("Ingestion source externe") as ps:
                        ps.tick(0.02, "Connexion & préparation…")
                        processed = {"rows": 0, "chunks": 0, "source_total": None}
                        def cb(total, rows_chunk, chunks_done):
                            if total is not None:
                                processed["source_total"] = total
                            if rows_chunk:
                                processed["rows"] += rows_chunk
                            processed["chunks"] = chunks_done
                            if processed["source_total"]:
                                ratio = min(0.98, processed["rows"] / processed["source_total"]) 
                            else:
                                ratio = min(0.98, 0.05 + 0.15*chunks_done)
                            ps.tick(ratio, f"Chunk {chunks_done} (+{rows_chunk:,} lignes) • total {processed['rows']:,}")
                        df = ingest_external_df(
                            ss.duck_con, ss.external_eng, sql_or_table, target_table,
                            is_table=is_table, mode=mode,
                            chunksize=(None if chunksize == 0 else int(chunksize)),
                            progress_cb=cb,
                            pre_count=do_precount,
                            count_query=None,
                            coerce_str_cols=str_cols_list,
                            coerce_bigint_cols=bigint_cols_list,
                        )
                        ps.tick(0.99, "Finalisation & aperçu…")
                        duckdb_count = ss.duck_con.execute(f"SELECT COUNT(*) FROM {quote_ident(target_table)}").fetchone()[0]
                        cols = st.columns(3)
                        with cols[0]:
                            st.metric("DuckDB COUNT", f"{duckdb_count:,}")
                        with cols[1]:
                            st.metric("Source COUNT", f"{(processed['source_total'] or 0):,}")
                        with cols[2]:
                            if processed["source_total"] is not None:
                                delta = duckdb_count - int(processed["source_total"])
                                st.metric("Delta (DuckDB - Source)", f"{delta:,}")
                            elif prior_count is not None:
                                delta = duckdb_count - prior_count
                                st.metric("Delta vs avant APPEND", f"{delta:,}")
                        st.success(f"Ingestion terminée: '{target_table}' • ~{processed['rows']:,} lignes (chunks={processed['chunks']})")
                        st.dataframe(df, use_container_width=True)
                        logger.info("Ingestion externe OK: table=%s, rows~=%d, chunks=%d", target_table, processed['rows'], processed['chunks'])
                except Exception as e:
                    st.error(f"Erreur ingestion: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur ingestion externe")

def render_attach_page():
    with TAB["Extensions ATTACH"]:
        st.subheader("ATTACH via extensions DuckDB (Postgres/MySQL/SQLite)")
        st.caption("Activez les extensions puis ATTACH pour joindre des bases externes.")
        db_type = st.selectbox("Type", ["postgres","mysql","sqlite"], key=k('att','type'))
        alias = st.text_input("Alias", value="ext_db", key=k('att','alias'))
        schema = st.text_input("Schéma (Postgres seulement)", value="public", key=k('att','schema')) if db_type == 'postgres' else None
        read_only = st.checkbox("Lecture seule", value=True, key=k('att','ro'))
        conn_str = st.text_input(
            "Connexion (libpq-style / fichier SQLite)",
            value=("host=localhost dbname=mydb user=user password=pass" if db_type!="sqlite" else "data/other.db"),
            key=k('att','conn')
        )
        if st.button("ATTACH", key=k('att','attach')):
            try:
                attach_external(ss.duck_con, db_type, conn_str, alias, schema=schema, read_only=read_only)
                st.success(f"Attaché: {alias}")
                st.write("Exemple: SELECT * FROM " + alias + ".ma_table LIMIT 10;")
                logger.info("ATTACH effectué: type=%s alias=%s", db_type, alias)
            except Exception as e:
                st.error(f"Erreur ATTACH: {e}")
                st.info("Vérifiez les extensions et l’accès réseau au dépôt.")
                logger.exception("Erreur ATTACH")

# -------- Snowflake (SSO) page --------

def render_snowflake_page():
    with TAB["Snowflake (SSO)"]:
        st.subheader("Snowflake (SSO) : requêtes & échanges avec DuckDB")

        # State & profils
        if "sf_ctx" not in ss:
            ss.sf_ctx = None
        state = load_state()
        profiles = list_sf_profiles(state)

        with st.expander("Connexion (SSO External Browser)", expanded=(ss.sf_ctx is None)):
            c0, c1 = st.columns([2, 2])
            with c0:
                # Profils
                prof_names = list(profiles.keys())
                selected_prof = st.selectbox("Profil enregistré", prof_names or ["(aucun profil)"])
                if prof_names and st.button("Charger le profil"):
                    prof = profiles.get(selected_prof, {})
                    ss.sf_account = prof.get("account", "")
                    ss.sf_user = prof.get("user", "")
                    ss.sf_role = prof.get("role", "")
                    ss.sf_wh = prof.get("warehouse", "")
                    ss.sf_db = prof.get("database", "")
                    ss.sf_schema = prof.get("schema", "")
                    st.success(f"Profil '{selected_prof}' chargé.")
                with st.expander("Enregistrer profil", expanded=False):
                    new_name = st.text_input("Nom du profil", value="default")
                    if st.button("Enregistrer", key=k('sf','save_prof')):
                        prof = {
                            "account": ss.get("sf_account", os.getenv("SNOWFLAKE_ACCOUNT", "")),
                            "user": ss.get("sf_user", os.getenv("SNOWFLAKE_USER", "")),
                            "role": ss.get("sf_role", os.getenv("SNOWFLAKE_ROLE", "")),
                            "warehouse": ss.get("sf_wh", os.getenv("SNOWFLAKE_WAREHOUSE", "")),
                            "database": ss.get("sf_db", os.getenv("SNOWFLAKE_DATABASE", "")),
                            "schema": ss.get("sf_schema", os.getenv("SNOWFLAKE_SCHEMA", "")),
                        }
                        save_sf_profile(state, new_name, prof)
                        save_state(state)
                        st.success(f"Profil '{new_name}' enregistré.")
                if prof_names and st.button("🗑️ Supprimer le profil"):
                    delete_sf_profile(state, selected_prof)
                    save_state(state)
                    st.success(f"Profil '{selected_prof}' supprimé.")

            with c1:
                acc = st.text_input("Account", value=ss.get("sf_account", os.getenv("SNOWFLAKE_ACCOUNT", "")))
                usr = st.text_input("User (UPN/email)", value=ss.get("sf_user", os.getenv("SNOWFLAKE_USER", "")))
                role = st.text_input("Role", value=ss.get("sf_role", os.getenv("SNOWFLAKE_ROLE", "")))
                wh = st.text_input("Warehouse", value=ss.get("sf_wh", os.getenv("SNOWFLAKE_WAREHOUSE", "")))
                db = st.text_input("Database", value=ss.get("sf_db", os.getenv("SNOWFLAKE_DATABASE", "")))
                sch = st.text_input("Schema", value=ss.get("sf_schema", os.getenv("SNOWFLAKE_SCHEMA", "")))
                timeout = st.number_input("Login timeout (s)", min_value=30, max_value=600, value=120, step=30)

                cbtn1, cbtn2 = st.columns(2)
                with cbtn1:
                    if st.button("🔐 Se connecter (SSO)", type="primary"):
                        try:
                            with st.status("Ouverture SSO (navigateur)…", expanded=True):
                                ctx = sf_connect_externalbrowser(
                                    account=acc, user=usr, role=role, warehouse=wh, database=db, schema=sch, login_timeout=int(timeout)
                                )
                                ss.sf_ctx = ctx
                                ss.sf_account, ss.sf_user, ss.sf_role, ss.sf_wh, ss.sf_db, ss.sf_schema = acc, usr, role, wh, db, sch
                                info = sf_current_context(ss.sf_ctx)
                                st.success("Connecté à Snowflake.")
                                st.caption(f"User: **{info['user']}**, Role: **{info['role']}**, WH: **{info['warehouse']}**, DB: **{info['database']}**, SCHEMA: **{info['schema']}**")
                        except Exception as e:
                            st.error(f"Erreur de connexion SSO: {e}")
                            st.info("Astuce: passe un *account* sans le suffixe '.snowflakecomputing.com' (ex: 'A4611928510171-C10').\nInstalle aussi l’extra 'secure-local-storage' pour éviter des popups SSO répétés.")
                            st.code('pip install "snowflake-connector-python[secure-local-storage]"')
                with cbtn2:
                    if st.button("🔌 Se déconnecter", disabled=(ss.sf_ctx is None)):
                        try:
                            if ss.sf_ctx is not None:
                                ss.sf_ctx.close()
                            ss.sf_ctx = None
                            st.info("Déconnecté.")
                        except Exception as e:
                            st.warning(f"Déconnexion: {e}")

        if ss.sf_ctx is None:
            st.info("Connecte-toi à Snowflake pour activer les sections ci-dessous.")
            return

        # Requêtage
        st.markdown("### 🔎 Requêter Snowflake")
        default_sql = "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION();"
        sql_text = st.text_area("SQL (SELECT/CTE)", height=160, value=default_sql, key=k('sf','sql'))
        cA, cB = st.columns([1,1])
        with cA:
            run_btn = st.button("▶️ Exécuter", key=k('sf','run'))
        with cB:
            max_rows = st.number_input("Max rows (0 = illimité)", min_value=0, value=0, step=1000)
        if run_btn:
            try:
                with progress_status("Exécution Snowflake") as ps:
                    ps.tick(0.1, "Envoi de la requête…")
                    df = sf_run_query(ss.sf_ctx, sql_text, max_rows=(None if max_rows == 0 else int(max_rows)))
                    ps.tick(0.9, f"Résultat reçu • {len(df):,} lignes")
                    st.dataframe(df.head(1000), use_container_width=True)
                    st.download_button("⬇️ Export CSV", data=df.to_csv(index=False).encode('utf-8'), file_name="snowflake_result.csv", mime="text/csv")
            except Exception as e:
                st.error(f"Erreur d'exécution: {e}")
                st.code(traceback.format_exc())

        st.divider()
        # SHOW TABLES
        st.markdown("### 📚 Tables Snowflake (SHOW TABLES)")
        colL, colR = st.columns(2)
        with colL:
            list_db = st.text_input("DB", value=ss.get("sf_db", ""))
        with colR:
            list_schema = st.text_input("Schéma", value=ss.get("sf_schema", ""))
        if st.button("Lister les tables"):
            try:
                df = sf_list_tables(ss.sf_ctx, database=list_db or None, schema=list_schema or None)
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Erreur SHOW TABLES: {e}")

        st.divider()
        # DuckDB -> Snowflake
        st.markdown("### ⬆️ Copier DuckDB → Snowflake")
        try:
            duck_tables = list_tables(ss.duck_con)['table_name'].tolist()
        except Exception:
            duck_tables = []
        tcol1, tcol2 = st.columns([1,2])
        with tcol1:
            duck_src = st.selectbox("Table source (DuckDB)", duck_tables, key=k('sf','duck_src'))
        with tcol2:
            colA, colB, colC = st.columns(3)
            with colA:
                tgt_db = st.text_input("DB cible", value=ss.get("sf_db", ""))
            with colB:
                tgt_schema = st.text_input("Schéma cible", value=ss.get("sf_schema", ""))
            with colC:
                tgt_table = st.text_input("Table cible", value=(duck_src or "my_table"))
            mode = st.selectbox("Mode", ["replace", "append", "fail"], index=0)
            chunk_rows = st.number_input("Chunk rows (0 = full)", min_value=0, value=0, step=100_000)
        if st.button("🚀 Envoyer vers Snowflake", disabled=not duck_src):
            try:
                with progress_status("DuckDB → Snowflake") as ps:
                    ps.tick(0.05, "Lecture DuckDB…")
                    rows, cols = sf_copy_duckdb_to_snowflake(
                        ss.duck_con, ss.sf_ctx, duck_table=duck_src,
                        target_database=tgt_db, target_schema=tgt_schema, target_table=tgt_table,
                        mode=mode, chunk_rows=(None if chunk_rows==0 else int(chunk_rows))
                    )
                    ps.tick(0.98, "Validation…")
                    st.success(f"Transfert terminé : {rows:,} lignes, {cols} colonnes → {tgt_db}.{tgt_schema}.{tgt_table}")
            except Exception as e:
                st.error(f"Erreur transfert DuckDB→Snowflake: {e}")
                st.code(traceback.format_exc())

        st.divider()
        # Snowflake -> DuckDB
        st.markdown("### ⬇️ Copier Snowflake → DuckDB")
        sA, sB = st.columns(2)
        with sA:
            src_db = st.text_input("DB source", value=ss.get("sf_db", ""))
            src_schema = st.text_input("Schéma source", value=ss.get("sf_schema", ""))
            src_table = st.text_input("Table source (optionnel si SQL)", value="")
        with sB:
            duck_target = st.text_input("Table DuckDB cible", value="sf_data")
            batch = st.number_input("Batch size", min_value=10_000, value=100_000, step=50_000)
            mode2 = st.selectbox("Mode d'écriture", ["replace","append","create"], index=0)
        sql_opt = st.text_area("OU SQL (SELECT)", height=120, value="", help="Laisse vide pour utiliser DB+Schéma+Table")
        if st.button("⬇️ Importer vers DuckDB"):
            try:
                with progress_status("Snowflake → DuckDB") as ps:
                    ps.tick(0.05, "Lecture Snowflake (batches)…")
                    rows, cols = sf_copy_snowflake_to_duckdb(
                        ss.sf_ctx, ss.duck_con,
                        src_database=src_db, src_schema=src_schema,
                        src_table=(src_table or None), sql=(sql_opt or None),
                        duck_target_table=duck_target, mode=mode2, batch_size=int(batch)
                    )
                    ps.tick(0.98, "Matérialisation DuckDB…")
                    st.success(f"Import terminé : {rows:,} lignes, {cols} colonnes → DuckDB.{duck_target}")
                    st.dataframe(ss.duck_con.execute(f'SELECT * FROM "{duck_target}" LIMIT 200').fetchdf(), use_container_width=True)
            except Exception as e:
                st.error(f"Erreur import Snowflake→DuckDB: {e}")
                st.code(traceback.format_exc())

# ----------------------
# Render all
# ----------------------

def render_all():
    render_sql_page()
    render_catalog_page()
    render_import_csv_page()
    render_sources_page()
    render_schema_page()
    render_comparer_page()
    render_profile_page()
    render_external_page()
    render_attach_page()
    render_snowflake_page()

render_all()
