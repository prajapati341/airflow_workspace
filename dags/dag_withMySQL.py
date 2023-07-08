from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


default_args={
    'owner':'sanjay mysql',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='using_MySQL_connection',
    default_args=default_args,
    description='MySQL database with DAG',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023,7,2)
) as dag:
    task1=MySqlOperator(
        task_id='create_mysql_table',
        mysql_conn_id='MySQL_Server_Ubuntu',
        sql="""
                        create table if not exists dags_run
                        (
                        dt date,
                        dag_id varchar(100),
                        primary key (dt,dag_id)
                        )
        """
    )

    insert_task=MySqlOperator(
        task_id='insert_into_mysql_table',
        mysql_conn_id='MySQL_Server_Ubuntu',
        sql="""
                insert into dags_run values ('{{ds}}','{{dag.dag_id}}')
        """
    )

    insert_task_mssql=MsSqlOperator(
        task_id='insert_into_mssql_table',
        mssql_conn_id='SQL_Server_window_remote',
        sql="""
                insert into dags_run values ('{{ds}}','{{dag.dag_id}}')
        """
    )



    delete_before_insert_task=MySqlOperator(
        task_id='delete_before_insert_into_mysql_table',
        mysql_conn_id='MySQL_Server_Ubuntu',
        sql="""
                delete from  dags_run where dt='{{ds}}' and dag_id='{{dag.dag_id}}'
        """
    )

    task1 >> delete_before_insert_task
    delete_before_insert_task >> insert_task
    task1 >> insert_task_mssql