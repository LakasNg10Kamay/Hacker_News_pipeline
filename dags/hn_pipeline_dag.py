from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException


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

    @task
    def transform(snapshot: str) -> str:
        from hn_lib.transform import run_transform
        return run_transform(snapshot)

    @task
    def load_to_databricks(parquet_path: str) -> None:
        from hn_lib.load_databricks import upload_to_volume
        if not upload_to_volume(parquet_path):
            raise AirflowSkipException("DATABRICKS_HOST / DATABRICKS_TOKEN not set; skipping upload")

    load_to_databricks(transform(extract()))


hn_pipeline()
