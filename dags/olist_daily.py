from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("olist_daily", start_date=datetime(2026, 1, 1), schedule="@daily",
         catchup=False, max_active_runs=1,
         tags=["portfolio", "olist"]) as dag:

    bronze = BashOperator(task_id="bronze_to_s3",
        bash_command="python /opt/airflow/ingest/to_bronze.py {{ ds }}")
    load_raw = BashOperator(task_id="load_raw_postgres",
        bash_command="PG_HOST=postgres python /opt/airflow/ingest/to_postgres.py {{ ds }}")
    dbt_run = BashOperator(task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .")
    dbt_test = BashOperator(task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .")

    bronze >> load_raw >> dbt_run >> dbt_test
