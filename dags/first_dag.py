from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime,timedelta


default_args={
    'owner':'sanjay airflow',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2021,7,29,2),
    schedule_interval='@daily'
) as dag:

    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello word"

    )

    task2= BashOperator(
        task_id='second_task',
        bash_command='echo "Hi im second task"',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )
    task3= BashOperator(
        task_id='third_task',
        bash_command='echo "Hi im third task"',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )
    task1 >> task2
    task1 >> task3


