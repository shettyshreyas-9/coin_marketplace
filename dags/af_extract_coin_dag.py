from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



default_args= {
    "owner":'airflow_SS',
    'start_date': days_ago(1),
    'catchup': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),

}

with DAG(
    dag_id= 'af_extract_coin_dag',
    default_args=default_args,
    description='A simple DAG to trigger a data extraction from api & saving it in cloud bucket',
    schedule_interval= '0 0 10 * *'
) as dag:
    
    extract_load_data= BashOperator(
        task_id='extract_load_data',
        bash_command='python3 /opt/airflow/scripts/extract_coin.py'
    )


    extract_load_data