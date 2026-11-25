from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator

DBT = "/opt/airflow/dbt_venv/bin/dbt"


@dag(
    schedule="0 */6 * * *",  # every 6 hours
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["hacker-news"],
)
def hn_pipeline():
    @task
    def extract() -> str:
        from hn_lib.extract import extract_top_stories
        return extract_top_stories()

    # dbt builds silver and gold in Postgres and runs every data test; any failed test fails this task
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd /opt/airflow/dbt && {DBT} build --profiles-dir . "
            "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
        ),
    )

    @task
    def export_parquet(snapshot: str) -> str:
        from hn_lib.export import export_silver_parquet
        return export_silver_parquet(snapshot)

    @task
    def load_to_databricks(parquet_path: str) -> None:
        from hn_lib.load_databricks import upload_to_volume
        if not upload_to_volume(parquet_path):
            raise AirflowSkipException("DATABRICKS_HOST or the token file is not set; skipping upload")

    snapshot = extract()
    exported = export_parquet(snapshot)
    snapshot >> dbt_build >> exported
    load_to_databricks(exported)


hn_pipeline()
