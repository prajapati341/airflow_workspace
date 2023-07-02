import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta



default_arg={
    'owner':'python owner',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'start_date': pendulum.yesterday()
}

def greet(ti):

    first_name=ti.xcom_pull(task_ids='get_name_push_pull',key='first_name')
    last_name=ti.xcom_pull(task_ids='get_name_push_pull',key='last_name')
    age=ti.xcom_pull(task_ids='get_age_push_pull',key='age')

    print('second function of push & pull, name is {} {} and age is {}'.format(first_name,last_name,age))





def get_name_xcom_push_pull(ti):
   ti.xcom_push(key='first_name',value='Sanjay')
   ti.xcom_push(key='last_name',value='Prajapati')


def get_age_xcom_push_pull(ti)   :
   ti.xcom_push(key='age',value=31)




#-------------------------------------------------------------------------------
def get_return_func():
   return 'Returned get name : Sanjay Prajapati'



def print_return_func(ti):
   last_name=ti.xcom_pull(task_ids='get_return')
   print(last_name)
#-------------------------------------------------------------------------------


with DAG(
    'Python_dag',
    default_args=default_arg,
    description='A simple tutorial DAG',
    start_date=datetime(2023,7,1),
    schedule_interval=timedelta(days=1),
    
) as dag:

 task1=PythonOperator(
    task_id='python_first_task',
    python_callable=greet
    #op_kwargs={'age':31}
 )


 task2=PythonOperator(
    task_id='get_name_push_pull',
    python_callable=get_name_xcom_push_pull
 )

 task3=PythonOperator(
    task_id='get_age_push_pull',
    python_callable=get_age_xcom_push_pull
 )


 task4=PythonOperator(
    task_id='get_return',
    python_callable=get_return_func
 )

 task5=PythonOperator(
    task_id='get_return_values',
    python_callable=print_return_func
 )

#  task3 >> task1
#  task2 >> task1
 
 [task2,task3] >> task1
 task1 >> task4
 task4 >> task5