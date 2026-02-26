FROM apache/airflow:3.1.7

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

COPY ./dags /opt/airflow/dags
COPY ./dbt_food_delivery /opt/airflow/dbt_food_delivery
COPY ./consumer /opt/airflow/consumer
COPY ./producer /opt/airflow/producer