import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import pendulum
from datetime import datetime,timedelta
from airflow.operators.email_operator import EmailOperator




default_arg={
    'owner':'python owner',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'start_date': pendulum.yesterday()
}


@task()
def get_data_mysql():

        print('test')
        sql_script="""select * from stock_data_interval limit 10"""
        hook = MySqlHook(mysql_conn_id="MySQL_Server_Ubuntu")
        df = hook.get_pandas_df(sql=sql_script)
        #print(df)
        return df


@task()
def insert_into_mssql(get_df):

        
        conn=BaseHook.get_connection('SQL_Server_window_remote')
        engine=create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?driver=ODBC Driver 17 for SQL Server')
        #engine = sa.create_engine("mssql+pyodbc://sa_window:1234@192.168.0.105:1433/Test?driver=ODBC Driver 17 for SQL Server")

        #print(type(get_df))
        get_df.to_sql('import_table',engine,if_exists='replace',index=False,chunksize=500 )
        # print(engine.connect())
        # if engine.connect():
        #         print('connection successful')

@task()
def replicate_table_mssql():
        
        conn=BaseHook.get_connection('SQL_Server_window_remote')
        engine=create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?driver=ODBC Driver 17 for SQL Server')

        get_conn=engine.connect()
        result=get_conn.execute('select * from import_table with (nolock)')
        df=pd.DataFrame(result)
        
        df.to_sql('import_table2',engine,if_exists='replace',index=False)


with DAG(dag_id="etl1",default_args=default_arg,schedule_interval="0 9 * * *", start_date=datetime(2023, 7, 9),catchup=False,  tags=["product_model"]) as dag:
        with TaskGroup('get_data_from_mysql',tooltip='Extract Data from mysql server') as task_extract_data:
                source_data=get_data_mysql()
                load_data=insert_into_mssql(source_data)
                source_data >> load_data
                
                

        with TaskGroup('replicate_tables',tooltip='Extract Data from') as replicate_table_task:
                replicate_table=replicate_table_mssql()
                
                #source_data >> load_data

                # send_mail=EmailOperator(
                #         task_id='EndTaskMail',
                #         to='prajapati341@gmail.com',
                #         subject='subject airflow test',
                #         html_content='''<h1>demo airflow mail</h1>'''
                # )

                replicate_table

        with TaskGroup('sending_mails',tooltip='send all mails') as send_mail_group_task:
                
                send_mail=EmailOperator(
                        task_id='EndTaskMail',
                        to='prajapati341@gmail.com',
                        subject='subject airflow test',
                        html_content='''<h1>demo airflow mail</h1>'''
                )
                send_mail
        
        task_extract_data >> replicate_table_task >> send_mail_group_task
        



            