
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email': ['shabahatalichishti@gmail.com'],     
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='ecommerce_sentiment_pipeline',
    default_args=default_args,
    description='End-to-end Big Data pipeline: data processing → sentiment analysis → PostgreSQL storage',
    schedule_interval='@daily',              
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['bigdata', 'spark', 'huggingface', 'postgres'],
) as dag:


    PROJECT_DIR = "/opt/airflow"
    DATA_PROCESSING_SCRIPT = f"{PROJECT_DIR}/spark_scripts/data_processing.py"
    HF_SENTIMENT_SCRIPT = f"{PROJECT_DIR}/spark_scripts/huggingface_sentiment.py"
    STORE_TO_PG_SCRIPT = f"{PROJECT_DIR}/postgressdb/store_to_postgres.py"

 
    data_processing = BashOperator(
        task_id='data_processing',
        bash_command=f'python {DATA_PROCESSING_SCRIPT}',
    )

   
    huggingface_sentiment = BashOperator(
        task_id='huggingface_sentiment',
        bash_command=f'python {HF_SENTIMENT_SCRIPT}',
    )

   
    store_to_postgres = BashOperator(
        task_id='store_to_postgres',
        bash_command=f'python {STORE_TO_PG_SCRIPT}',
    )

   
    data_processing >> huggingface_sentiment >> store_to_postgres
