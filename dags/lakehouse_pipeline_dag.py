from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="medallion_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Medallion Lakehouse: Event Gen -> Bronze -> Silver -> Gold",
    schedule_interval=None,  # Trigger on-demand or set to '@daily'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "delta-lake", "pyspark"],
) as dag:

    # Task 1: Generate landing batch of dirty raw events
    generate_events = BashOperator(
        task_id="generate_events",
        bash_command="python /opt/airflow/jobs/generator/generate_events.py",
    )

    # Task 2: Ingest into Bronze Delta + Quarantine
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="python /opt/airflow/jobs/bronze/ingest_bronze.py",
    )

    # Task 3: Clean, Mask PII, and Deduplicate into Silver Delta
    clean_silver = BashOperator(
        task_id="clean_silver",
        bash_command="python /opt/airflow/jobs/silver/clean_silver.py",
    )

    # Task 4: Aggregate into Gold Business Marts
    aggregate_gold = BashOperator(
        task_id="aggregate_gold",
        bash_command="python /opt/airflow/jobs/gold/aggregate_gold.py",
    )

    # Dependency Pipeline Flow:
    generate_events >> ingest_bronze >> clean_silver >> aggregate_gold