"""
DuckDB SQL Explorer
- Convention de nommage des clés Streamlit (préfixes par page)
- Fichier modulaire (fonctions par onglet)
- Onglet 'Atelier (Comparer)' renommé en 'Comparer'
- Menus réordonnés pour une navigation plus cohérente
- Fonctionnalités conservées: Comparer, SQL, Catalogue (copier/dupliquer/exécuter/export/import),
  Importer CSV (bouton explicite de chargement), Sources, Schéma, Profilage, Source externe, ATTACH
- Logging (fichier + console)
"""
import traceback
import os
import json
import logging
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
    persist_df,
    init_logging,
)

# ------------------------------
# Config & Naming Convention
# ------------------------------

st.set_page_config(page_title="DuckDB SQL Explorer", page_icon="🦆", layout="wide")
ss = st.session_state

# Convention de nommage des clés pour les widgets Streamlit
# Préfixes par page pour garantir l'unicité globale
KEY_PREFIX = {
    'cmp': 'cmp',     # Comparer
    'sql': 'sql',     # SQL Workbench
    'cat': 'cat',     # Catalogue
    'csv': 'csv',     # Importer CSV
    'src': 'src',     # Sources
    'sch': 'sch',     # Schéma
    'pro': 'pro',     # Profilage
    'ext': 'ext',     # Source externe
    'att': 'att',     # ATTACH
}

def k(page: str, name: str) -> str:
    """Génère une clé unique pour les widgets: <prefixe_page>_<nom>"""
    return f"{KEY_PREFIX.get(page, page)}_{name}"

# ------------------------------
# Logging
# ------------------------------

if 'logger_initialized' not in ss:
    os.makedirs('logs', exist_ok=True)
    init_logging(log_dir='logs', level=logging.INFO)
    ss.logger_initialized = True
logger = logging.getLogger('duckapp')

# ------------------------------
# Connexion DuckDB & état global
# ------------------------------

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

# ------------------------------
# Helpers
# ------------------------------

def _download_button_csv(df: pd.DataFrame, filename: str, label: str):
    st.download_button(label=label, data=df.to_csv(index=False).encode('utf-8'), file_name=filename, mime='text/csv')

# ------------------------------
# Barre latérale (Backend & Extensions)
# ------------------------------

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
            st.info("Astuce: certaines plateformes/réseaux bloquent le dépôt des extensions. Essayez 'FROM core' ou un repo custom.")
            st.code(traceback.format_exc())
            logger.exception("Erreur d'activation des extensions")

# ------------------------------
# Titre
# ------------------------------

st.title("🦆 DuckDB SQL Explorer")
st.caption("Comparer, requêter, profiler et gérer vos données DuckDB en toute simplicité.")

# ------------------------------
# Menus (réordonnés)
# ------------------------------

TAB_ORDER = [
    "SQL",
    "Comparer",
    "Catalogue",
    "Importer CSV",
    "Sources",
    "Schéma",
    "Profilage",
    "Source externe → DuckDB",
    "Extensions ATTACH"
]

# Création des tabs et mapping nom → tab
_TABS = st.tabs(TAB_ORDER)
TAB = {name: tab for name, tab in zip(TAB_ORDER, _TABS)}



# ------------------------------
# Pages (fonctions modulaires)
# ------------------------------

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
                        st.dataframe(df_prev, width='stretch')
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
                        st.dataframe(df_prev, width='stretch')
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
                            st.dataframe(df_ab, width='stretch')
                            _download_button_csv(df_ab, "diff_A_minus_B.csv", "📥 Exporter (CSV)")
                        with t2:
                            st.dataframe(df_ba, width='stretch')
                            _download_button_csv(df_ba, "diff_B_minus_A.csv", "📥 Exporter (CSV)")
                        logger.info("Comparaison A vs B effectuée: %s vs %s", prep_a, prep_b)
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Erreur comparaison: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur comparaison A vs B")


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
                        st.dataframe(page_df, width='stretch')
                        _download_button_csv(full_df, "resultat_sql.csv", "📥 Télécharger (CSV)")
                    with tabs[1]:
                        if exp_df is not None and not exp_df.empty:
                            st.dataframe(exp_df, width='stretch')
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
                st.dataframe(qdf, width='stretch')
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
                new_name = st.text_input("Nom de la copie (clone)", value=f"{sel}_copy" if sel else "new_query", key=k('cat','clone_name'))
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
                        st.dataframe(result_df.head(200), width='stretch')
                        _download_button_csv(result_df, f"resultat_{sel}.csv", "📥 Export résultat (CSV)")
                        logger.info("Requête exécutée depuis catalogue: %s", sel)
                    except Exception as e:
                        st.error(f"Erreur exécution: {e}")
                        st.code(traceback.format_exc())

            st.markdown("---")
            st.subheader("Export / Import du catalogue")
            exp_col, imp_col = st.columns(2)
            with exp_col:
                fmt_exp = st.selectbox("Format export", ["csv","json"], index=0, key=k('cat','fmt_exp'))
                out_dir = st.text_input("Dossier de sortie", value="exports", key=k('cat','out_dir'))
                out_name = st.text_input("Nom de fichier", value=f"catalogue_requetes.{fmt_exp}", key=k('cat','out_name'))
                if st.button("⬇️ Exporter", key=k('cat','export')):
                    try:
                        os.makedirs(out_dir, exist_ok=True)
                        path = os.path.join(out_dir, out_name)
                        export_query_catalog(ss.duck_con, path, fmt=fmt_exp)
                        st.success(f"Catalogue exporté: {path}")
                        logger.info("Catalogue exporté: %s", path)
                    except Exception as e:
                        st.error(f"Erreur export: {e}")
                        st.code(traceback.format_exc())
            with imp_col:
                fmt_imp = st.selectbox("Format import", ["csv","json"], index=0, key=k('cat','fmt_imp'))
                up = st.file_uploader("Fichier à importer", type=[fmt_imp], key=k('cat','file'))
                mode_imp = st.selectbox("Mode import", ["append","replace"], index=0, key=k('cat','mode_imp'))
                if st.button("⬆️ Importer", key=k('cat','import')) and up is not None:
                    try:
                        tmp_dir = "imports"
                        os.makedirs(tmp_dir, exist_ok=True)
                        tmp_path = os.path.join(tmp_dir, f"catalog_import.{fmt_imp}")
                        with open(tmp_path, "wb") as f:
                            f.write(up.getbuffer())
                        import_query_catalog(ss.duck_con, tmp_path, fmt=fmt_imp, mode=mode_imp)
                        st.success("Catalogue importé.")
                        logger.info("Catalogue importé: %s (%s)", tmp_path, mode_imp)
                    except Exception as e:
                        st.error(f"Erreur import: {e}")
                        st.code(traceback.format_exc())

            _download_button_csv(qdf, "catalogue_requetes.csv", "⬇️ Exporter la liste (CSV)")
        except Exception as e:
            st.error(f"Erreur catalogue: {e}")
            st.code(traceback.format_exc())
            logger.exception("Erreur catalogue")


def render_import_csv_page():
    with TAB["Importer CSV"]:
        st.subheader("Importer un fichier CSV → table DuckDB")
        st.caption("Séparateurs multi-caractères (ex. '||'), encodages variés, bouton explicite pour charger.")
        uploaded = st.file_uploader("Sélectionner un fichier CSV", type=["csv"], key=k('csv','uploader'))
        col_l, col_r = st.columns(2)
        with col_l:
            sep = st.text_input("Séparateur", value=",", key=k('csv','sep'))
            encoding = st.text_input("Encodage", value="utf-8", key=k('csv','encoding'))
            header_row = st.number_input("Index ligne d’en-tête (0 = première)", min_value=-1, value=0, step=1, key=k('csv','header'))
        with col_r:
            quotechar = st.text_input("Quote char (vide = désactivé)", value='"', key=k('csv','quote'))
            escapechar = st.text_input("Escape char (vide = désactivé)", value="", key=k('csv','escape'))
            parse_mode = st.selectbox("Mode parsing séparateur multi-caractères", options=["literal", "regex"], index=0, key=k('csv','parse_mode'))
        st.markdown('---')
        with st.expander('Options avancées CSV', expanded=False):
            low_memory = st.checkbox('low_memory (optimiser mémoire)', value=False, help='Découpage par chunks; peut générer des DtypeWarning si colonnes mixtes.', key=k('csv','low_memory'))
            dtype_json = st.text_area('dtype (JSON facultatif)', value='', help='Ex: {"col1": "string", "col2": "Int64"}', key=k('csv','dtype_json'))
            try:
                dtype_map = json.loads(dtype_json) if dtype_json.strip() else None
            except Exception:
                st.warning('dtype JSON invalide — ignoré')
                dtype_map = None
        target_table = st.text_input("Nom de la table cible", value="import_csv", key=k('csv','target'))
        mode = st.selectbox("Mode d'écriture", options=["replace","append","create"], index=0, key=k('csv','mode'))
        if st.button("📂 Charger le fichier CSV", key=k('csv','load_btn')):
            if uploaded is None:
                st.warning("Aucun fichier sélectionné.")
            else:
                try:
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
                    ss.csv_preview_df = df
                    ss.csv_preview_meta = {
                        'rows': len(df), 'cols': len(df.columns),
                        'sep': sep, 'encoding': encoding, 'header': header_row,
                        'parse_mode': parse_mode, 'low_memory': low_memory
                    }
                    st.success(f"Fichier chargé : {len(df):,} lignes, {len(df.columns)} colonnes")
                    st.dataframe(df.head(200), width='stretch')
                    logger.info("CSV CHARGÉ (%d lignes, %d colonnes)", len(df), len(df.columns))
                except Exception as e:
                    st.error(f"Erreur de lecture CSV: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur lecture CSV")
        if st.button("Importer → DuckDB", key=k('csv','import_btn')):
            if ss.csv_preview_df is None:
                st.warning("Charge d'abord le fichier CSV (bouton ci-dessus).")
            else:
                try:
                    persist_df(ss.duck_con, target_table, ss.csv_preview_df, mode=mode)
                    st.success(f"Import terminé : table '{target_table}' ({ss.csv_preview_meta['rows']:,} lignes).")
                    st.caption("Vous pouvez vérifier la table dans l’onglet 'Sources'.")
                    logger.info("CSV importé dans table: %s (mode=%s)", target_table, mode)
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
                st.dataframe(tbl_df, width='stretch')
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
                    st.dataframe(schema_df, width='stretch')
                    st.caption(f"Lignes: {table_rowcount(ss.duck_con, table):,}")
                except Exception as e:
                    st.error(f"Erreur schéma: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur schéma table: %s", table)


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
                        st.dataframe(prof['schema_df'], width='stretch')
                    with sub_tabs[1]:
                        if not prof['numeric_stats_df'].empty:
                            st.dataframe(prof['numeric_stats_df'], width='stretch')
                        else:
                            st.info("Aucune colonne numérique.")
                    with sub_tabs[2]:
                        if not prof['categorical_stats_df'].empty:
                            st.dataframe(prof['categorical_stats_df'], width='stretch')
                        else:
                            st.info("Aucune colonne catégorielle.")
                    with sub_tabs[3]:
                        st.dataframe(prof['nulls_df'], width='stretch')
                    logger.info("Profilage effectué sur table: %s", table)
                except Exception as e:
                    st.error(f"Erreur profilage: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur profilage table: %s", table)


def render_external_page():
    with TAB["Source externe → DuckDB"]:
        st.subheader("Connexion source externe (SQLAlchemy) puis ingestion dans DuckDB")
        st.caption("Exemples d'URL : postgresql+psycopg2://user:pass@host:5432/dbname, mysql+pymysql://..., mssql+pyodbc://...")
        sa_url = st.text_input("SQLAlchemy URL", value="", key=k('ext','url'))
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
            if st.button("Ingestion → DuckDB", key=k('ext','ingest')):
                try:
                    df = ingest_external_df(ss.duck_con, ss.external_eng, sql_or_table, target_table, is_table=is_table, mode=mode)
                    st.success(f"Ingestion terminée: table '{target_table}' (lignes: {len(df):,}, colonnes: {len(df.columns)})")
                    st.dataframe(df.head(200), width='stretch')
                    logger.info("Ingestion externe vers DuckDB: %s (%d lignes)", target_table, len(df))
                except Exception as e:
                    st.error(f"Erreur ingestion: {e}")
                    st.code(traceback.format_exc())
                    logger.exception("Erreur ingestion externe")


def render_attach_page():
    with TAB["Extensions ATTACH"]:
        st.subheader("ATTACH via extensions DuckDB (Postgres/MySQL/SQLite)")
        st.caption("Les extensions sont activables depuis la barre latérale. Utilisez ATTACH pour lire/écrire en direct.")
        db_type = st.selectbox("Type", ["postgres","mysql","sqlite"], key=k('att','type'))
        alias = st.text_input("Alias", value="ext_db", key=k('att','alias'))
        schema = st.text_input("Schéma (Postgres seulement)", value="public", key=k('att','schema')) if db_type == 'postgres' else None
        read_only = st.checkbox("Lecture seule", value=True, key=k('att','ro'))
        conn_str = st.text_input("Connexion (libpq-style / fichier SQLite)", value=("host=localhost dbname=mydb user=user password=pass" if db_type!="sqlite" else "data/other.db"), key=k('att','conn'))
        if st.button("ATTACH", key=k('att','attach')):
            try:
                attach_external(ss.duck_con, db_type, conn_str, alias, schema=schema, read_only=read_only)
                st.success(f"Attaché: {alias}")
                st.write("Exemple: SELECT * FROM "+alias+".ma_table LIMIT 10;")
                logger.info("ATTACH effectué: type=%s alias=%s", db_type, alias)
            except Exception as e:
                st.error(f"Erreur ATTACH: {e}")
                st.info("Vérifiez: 1) extension 'postgres' (pas 'postgres_scanner'), 2) accès réseau au dépôt des extensions, 3) essayez INSTALL ... FROM core.")
                logger.exception("Erreur ATTACH")

# ------------------------------
# Rendu
# ------------------------------
render_sql_page()
render_catalog_page()
render_import_csv_page()
render_sources_page()
render_schema_page()
render_comparer_page()
render_profile_page()
render_external_page()
render_attach_page()