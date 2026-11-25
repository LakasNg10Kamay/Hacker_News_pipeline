FROM apache/airflow:2.10.5-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# dbt gets its own virtual environment so its dependencies can't clash with Airflow's
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.9 dbt-postgres==1.8.2
