from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'schedule': None,
    'catchup': False
}

with DAG('hello_world_dag', default_args=default_args) as dag:
    task_hello = BashOperator(
        task_id='hello_task',
        bash_command='echo "hello world"'
    )
