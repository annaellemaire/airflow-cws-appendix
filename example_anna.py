from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'schedule': None,
    'catchup': False
}

with DAG('example_annaelle', default_args=default_args,) as dag:
    task_start = BashOperator(
        task_id='start_task',
        bash_command='echo "Start task"'
    )

    for i in range(3):
        task_dynamic = BashOperator(
            task_id=f'dynamic_task_{i}',
            bash_command=f'echo "Dynamic task {i}"'
        )
        task_start >> task_dynamic

    task_end = BashOperator(
        task_id='end_task',
        bash_command='echo "End task"'
    )
    task_start >> task_end
