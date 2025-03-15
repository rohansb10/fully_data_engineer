from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    start_task >> end_task