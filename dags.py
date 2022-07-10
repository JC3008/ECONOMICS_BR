from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import extract_html
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

with DAG(
    dag_id = 'Economics_BR',
    schedule_interval = '5 14 * * *',
    start_date=datetime(2022,7,10),
    catchup=False,
    tags=['Economics_BR'],
) as dag:

getting_data = PythonOperator(
        task_id='extract_html',
        python_callable=extract_html,
        
    )