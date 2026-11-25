# Hacker News Data Pipeline

Batch data pipeline that snapshots the Hacker News front page every 6 hours. Airflow loads raw data into PostgreSQL (bronze); dbt transforms and tests it into silver and gold schemas in Postgres; the silver layer is exported as Parquet, and Databricks then uses PySpark to build Delta tables tracking story scores, ranks, and posting-time trends. Everything runs locally in Docker.

**Stack:** Python, Apache Airflow, PostgreSQL, dbt, Docker, Databricks Free Edition (PySpark + Delta Lake), pgAdmin

## Why I built this

I work as a database engineer with SQL Server, where I build ETL/ELT pipelines using a Bronze, Silver, and Gold structure. I built this project to apply the same approach with modern, open-source and cloud data tools: Apache Airflow for orchestration, dbt for transformation and testing, and Databricks with PySpark and Delta Lake for the cloud layer.

The goals were to:

- **Get hands-on experience with tools used in data engineering roles**, especially Airflow, dbt, Spark, and Databricks, which I don't use at work.
- **Build a complete, working pipeline end to end**, from a live API to cloud tables, including scheduling, data quality checks, and secure handling of secrets.
- **Keep it free to run**, using Docker locally and Databricks Free Edition instead of paid cloud services.

Hacker News was a good data source for this because its API is free and needs no key, and its front page changes constantly. That makes it possible to answer real questions with the data: which websites reach the front page most often, how a story's score and rank change over time, and whether the hour a story is posted affects how well it does.

## Architecture

```mermaid
flowchart LR
    API["Hacker News API"]

    subgraph local["Docker (local)"]
        AF{{"Apache Airflow<br/>runs every 6 hours"}}
        BR[("PostgreSQL<br/>Bronze: raw JSON")]
        DBT["dbt<br/>models + tests"]
        SG[("PostgreSQL<br/>Silver + Gold schemas")]
        PQ["Parquet file"]
    end

    subgraph cloud["Databricks Free Edition"]
        VOL[("Volume<br/>landing/silver")]
        SPK["PySpark notebook"]
        DELTA[("Delta tables<br/>Silver + Gold")]
    end

    API -->|extract| BR
    BR --> DBT -->|build| SG
    SG -->|export| PQ
    PQ -->|load via Files API| VOL
    VOL --> SPK --> DELTA

    AF -.->|orchestrates| BR
    AF -.-> DBT
    AF -.-> PQ
    AF -.-> VOL
```

| Layer | Where | Tables |
|---|---|---|
| Bronze | PostgreSQL `public` | `raw_items` (raw API JSON, one row per story per snapshot) |
| Silver | PostgreSQL `silver` (dbt), Databricks | `silver_stories` (parsed, cleaned, tested) |
| Gold | PostgreSQL `gold` (dbt), Databricks | `gold_story_latest`, `gold_domain_stats`, `gold_posting_hour` |

Airflow DAG: `extract → dbt_build → export_parquet → load_to_databricks`

### What each part does

- **Airflow** schedules and runs the three tasks in order, retrying failed tasks automatically.
- **PostgreSQL (bronze)** stores every API response untouched as JSONB. Each run adds a new snapshot in a single transaction, and a primary key on `(item_id, snapshot_ts)` prevents duplicates.
- **dbt (silver and gold)** is the transformation layer. Its SQL models run inside Postgres and:
  - parse the raw JSON into columns (title, URL, author, score, comment count, posting time);
  - extract each story's website from its URL (posts without a URL, such as Ask HN, are labeled `news.ycombinator.com`);
  - drop non-story items and deleted or dead posts;
  - build the gold tables in a separate `gold` schema;
  - run data tests (`unique`, `not_null`, and a custom test that the newest snapshot produced silver rows). If any test fails, the Airflow task fails.
- **Export** reads the current snapshot from `silver.silver_stories` and writes it as a Parquet file (pyarrow) for Databricks.
- **Databricks** receives the Parquet files in a Unity Catalog volume, and a PySpark notebook builds Delta tables for analysis.

## Results

<!-- After the pipeline has collected a few days of data, add the chart:
![Average peak score by posting hour (UTC)](images/posting_hour_chart.png)
-->

The `gold_posting_hour` table shows the average peak score of front-page stories by the hour they were posted (UTC). The pattern becomes more reliable as more snapshots are collected.

## Run it

You need Docker Desktop.

1. Copy `.env.example` to `.env`. Leave `DATABRICKS_HOST` blank for now if you only want the local part.
2. Create the secret files described under **Secrets** below (the token file can be empty for now). Docker needs both files to exist.
3. On Linux/macOS, set your user ID so Airflow can write to `data/`: `echo "AIRFLOW_UID=$(id -u)" >> .env` (skip on Windows).
4. Start everything:
   ```
   docker compose up --build
   ```
5. Get the Airflow password:
   ```
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```
6. Open http://localhost:8080 and log in as `admin`.
7. Turn on the `hn_pipeline` DAG and trigger it. Without Databricks settings, the last task is skipped and the local pipeline still completes.

### Look at the local results

Query the gold tables in pgAdmin (below), or from the terminal:
```
docker compose exec postgres psql -U airflow -d hn -c "SELECT * FROM gold.gold_domain_stats LIMIT 15;"
```

### dbt documentation and lineage

Generate the docs and open them at http://localhost:8085:
```
docker compose exec airflow bash -c "cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt docs generate --profiles-dir . && cd target && python -m http.server 8085"
```
Press **Ctrl + C** to stop the docs server. The lineage graph shows how `raw_items` flows into the silver and gold models.

### Browse Postgres with pgAdmin

Open http://localhost:8082 and log in with `PGADMIN_EMAIL` from `.env` and the password from your pgAdmin secret file. Register a server with Host `postgres`, Port `5432`, Username `airflow`, Password `airflow`, then open **Databases → hn → Schemas**: `public` holds bronze (`raw_items`), and `silver` and `gold` hold the dbt models.

Postgres is also reachable from your machine on port **5433** (same user and password), for example with DBeaver.

## Add Databricks Free Edition

1. Sign up for Databricks Free Edition.
2. Import `databricks/hn_gold_notebook.py` into your workspace and run **only the first (setup) cell** on Serverless compute. It creates the `workspace.hn` schema and the `landing` volume.
3. Create a personal access token in your user settings (Developer section), with the **files** scope set to write access.
4. Put the token in the token secret file (see **Secrets**) and set `DATABRICKS_HOST` in `.env` to your workspace URL, ending at `.com`. Then restart: `docker compose down` and `docker compose up -d`.
5. Trigger the DAG. The Parquet file lands in `/Volumes/workspace/hn/landing/silver/`.
6. Run the full notebook to build the Delta tables.
7. Optional: create a Databricks job that runs the notebook, put its ID in `DATABRICKS_JOB_ID`, and Airflow will trigger it after each upload (the token then also needs a jobs scope with write access).

## Secrets

No passwords or tokens are stored in `.env` or anywhere in the project folder.

| Secret | File (outside the project) | Mounted in the container at |
|---|---|---|
| Databricks token | `%USERPROFILE%\.hn-secrets\databricks_token.txt` | `/run/secrets/databricks_token` |
| pgAdmin password | `%USERPROFILE%\.hn-secrets\pgadmin_password.txt` | `/run/secrets/pgadmin_password` |

- Docker Compose mounts these files as read-only secrets, so they never appear as environment variables or in Git.
- Airflow, Postgres, and pgAdmin only listen on `127.0.0.1`, so other devices on your network can't reach them.
- The Databricks token is least-privilege (files scope only) and short-lived; revoke it in Databricks if it ever leaks.

Create the files on Windows (PowerShell):
```
mkdir $env:USERPROFILE\.hn-secrets
notepad $env:USERPROFILE\.hn-secrets\databricks_token.txt
notepad $env:USERPROFILE\.hn-secrets\pgadmin_password.txt
```
Paste the value, save, and close each one. Notepad keeps the values out of your PowerShell history.
(On macOS/Linux, set `USERPROFILE=$HOME` before running Docker Compose, or edit the `secrets` paths in `docker-compose.yml`.)

The Postgres password itself is the default `airflow`. That is acceptable because Postgres only accepts connections from this computer; a shared or production setup would move it to a secret as well.

## Data quality checks

- **Extract** fails the run if the API returns nothing, and inserts everything in one transaction (no partial loads).
- **dbt tests** fail the run if keys are missing or duplicated, or if the newest snapshot produced no silver rows.
- **Export** fails the run if there are no silver rows for the snapshot.
- **The notebook** asserts there is data and no null keys before writing Delta tables.

## Design choices

- **Snapshots, not overwrites:** each run keeps a new copy of every story, so you can see how score and rank change over time.
- **Postgres as bronze:** raw JSON is kept untouched (JSONB), so silver can be rebuilt any time, even for snapshots the API can no longer return.
- **dbt on Postgres for transforms:** each layer is a version-controlled SQL model with tests and documentation, and all three layers live in one database, which keeps the setup simple and easy to inspect. Gold tables are built in both dbt (SQL) and Databricks (PySpark) to show the same logic on both platforms.
- **dbt in its own virtual environment:** keeps dbt's dependencies from clashing with Airflow's inside the same Docker image.
- **Databricks for the cloud layer:** Delta tables and PySpark at zero cost on Free Edition.
- **Logs on disk:** Airflow task logs are saved to `logs/` so they survive container restarts (ignored by Git).
