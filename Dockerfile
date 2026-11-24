FROM apache/airflow:2.10.5-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# Pre-install DuckDB's Postgres extension so the pipeline doesn't download it at run time
RUN python -c "import duckdb; duckdb.sql('INSTALL postgres')"
