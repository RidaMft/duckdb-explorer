# 🦆 SQL Explorer – Multi‑source (DuckDB + Streamlit)

Un mini‑**Dremio‑like** open‑source pour **explorer, joindre et profiler** des données locales et externes, avec **DuckDB** comme backend persistant.

> **Points clés**
> - Chargez **plusieurs CSV** (séparateurs multi‑caractères supportés)
> - Connectez des **bases externes** via **SQLAlchemy** *ou* directement via les **extensions DuckDB** (Postgres/MySQL/SQLite)
> - Exécutez des **requêtes SQL** entre **toutes** vos sources
> - Inspectez **schémas** et **stats** (profilage)
> - **Comparez** et **fusionnez** 2 tables
> - **Exportez** vos tables en **Parquet/CSV** et **compactez** la base (VACUUM)

---

## 🎯 Pourquoi utiliser ce projet ?

### 1) Rapidité & simplicité
**DuckDB** est une base **embarquée** optimisée OLAP : zero‑serveur, très rapide sur fichiers locaux (CSV/Parquet/JSON) et DataFrames. Vous obtenez une **expérience interactive** façon “SQL Workbench” sans infra lourde.

### 2) Multi‑sources, requêtes croisées
Joignez en SQL des tables **CSV**, des résultats **ingérés via SQLAlchemy**, et des **schémas attachés** en live (Postgres/MySQL/SQLite) via les **extensions DuckDB** → un seul **catalogue DuckDB**.

### 3) Profilage & gouvernance légère
Inspectez vos colonnes (**types, nulls, stats numériques, top catégories**), comparez 2 tables (anti‑join), fusionnez (INNER/LEFT/RIGHT/FULL), puis **exportez**/**versionnez**.

### 4) Dev‑friendly & portable
Tout est **Python + Streamlit** : facile à étendre, connecteurs SQLAlchemy classiques, **Docker** pour un démarrage en 1 commande.

---

## 🧱 Architecture

- **Frontend** : Streamlit (onglets *Sources, SQL, Schéma, Profilage, Comparer, Fusionner, Source externe, Extensions ATTACH*)
- **Backend** : DuckDB persistant (`data/catalog.duckdb`) + utilitaires (profilage, export, attach, ingestion SQLAlchemy)
- **Extensions DuckDB** : `postgres`, `mysql`, `sqlite`, `httpfs`, `json` (activables depuis l’UI)

---

## 🚀 Démarrage (local)

```bash
# 1) Créez et activez votre venv (recommandé)
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2) Installez les dépendances
pip install -r requirements.txt

# 3) Lancez l'app
streamlit run app.py
```

Ouvrez l’URL affichée (par défaut http://localhost:8501). Dans la barre latérale :
1) **(Re)connecter** DuckDB (chemin modifiable, ex. `data/catalog.duckdb`)  
2) **Activer extensions** (Postgres/MySQL/SQLite/httpfs/json)  
3) **Charger CSV (multi)**  

---

## 🐳 Démarrage avec Docker

```bash
# Build
docker build -t duckdb-sql-explorer .

# Run (port 8501, volumes pour persistance et exports)
docker run -it --rm \
  -p 8501:8501 \
  -v $PWD/data:/app/data \
  -v $PWD/exports:/app/exports \
  --name duckdb-sql-explorer \
  duckdb-sql-explorer
```

Ou via **docker-compose** :

```bash
docker compose up --build -d
```

> Les **extensions DuckDB** sont installées/chargées dynamiquement (nécessitent l’accès Internet au dépôt des extensions). Vous pouvez les activer depuis l’UI.

---

## 🔌 Connexions externes

### Via SQLAlchemy (ingestion → DuckDB)
Onglet **Source externe → DuckDB** : renseignez une **URL SQLAlchemy** (ex. `postgresql+psycopg2://user:pass@host:5432/db`), puis saisissez une **table** *ou* une **requête SELECT** ; ciblez une table DuckDB (`replace|append|create`).

### Via extensions DuckDB (ATTACH en live)
Onglet **Extensions ATTACH** : choisissez le type (`postgres|mysql|sqlite`), l’alias et la connexion/libpq‑string (ou le fichier SQLite). Une fois attachée, requêtez `alias.schema.table` directement (sans ingestion).

---

## 📦 Export & maintenance

- **Export** : *Sources → Exporter une table* → Parquet/CSV (`exports/…`)  
- **Compactage** : bouton **VACUUM** pour réduire l’empreinte disque de `data/catalog.duckdb`

---

## 🔐 Secrets & sécurité

- Évitez de mettre des mots de passe en dur dans l’UI : préférez **variables d’environnement** / fichiers **`.env`** (supportés par Docker Compose).  
- Postgres/MySQL via ATTACH : utilisez de préférence des **chaînes libpq** et limitez à un **schéma** si la base est volumineuse.

---

## ⚠️ Limites & bonnes pratiques

- **ATTACH (live)** sur bases distantes est pratique, mais pour des workloads importants, **ingérez** (matérialisez) dans DuckDB pour gagner en stabilité et reproductibilité.
- Sur CSV volumineux avec **séparateurs multi‑caractères**, gardez le mode *literal* (plus sûr pour les quotes).

---

## 🤝 Contribuer
PRs/Issues bienvenues ! Merci d’ouvrir une issue avec un **exemple minimal** (fichier, requête, log) pour accélérer le debug.

---

## 📜 Licence
MIT
