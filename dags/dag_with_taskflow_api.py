from datetime import datetime,timedelta
from airflow.decorators import dag,task


default_args={
    'owner':'sanjay api',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}