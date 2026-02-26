from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# If you want to call your consumer function directly

default_args = {
    "owner": "sanjeev",
    "retries": 1,
}

with DAG(
    dag_id="food_delivery_pipeline",
    default_args=default_args,
    description="Kafka -> Snowflake -> dbt pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["kafka_streaming", "snowflake"],
) as dag:

    # 2️⃣ dbt debug (optional but useful)
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="cd /opt/airflow/dbt_food_delivery && dbt debug",
    )

    # 3️⃣ dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_food_delivery && dbt run",
    )

    dbt_debug >> dbt_run