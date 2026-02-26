from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "sanjeev",
    "retries": 1,
}

with DAG(
    dag_id="generate_orders",
    tags=["ingest", "kafka_streaming"],
    default_args=default_args,
    description="Simulating orders and consuming them",
    start_date=datetime(2026, 2, 22),
    schedule="@daily",
    catchup=False,
) as dag:

    simulate = BashOperator(
        task_id="simulate",
        bash_command="python /opt/airflow/producer/app.py"
    )

    consume = BashOperator(
        task_id="consume",
        bash_command="cd /opt/airflow && python -m consumer.app"
    )

    simulate >> consume 