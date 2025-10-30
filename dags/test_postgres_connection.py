from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    test_conn = PostgresOperator(
        task_id="test_conn",
        postgres_conn_id="my_postgres",
        sql="SELECT version();"
    )
