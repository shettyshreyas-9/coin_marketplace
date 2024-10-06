from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow_SS',
    'start_date': days_ago(1),
    'retries': 1,
}

# Function to pull the detected file from XCom and format the input path for Dataflow
def get_detected_file(ti):
    detected_files = ti.xcom_pull(task_ids='check_file_in_gcs')
    if detected_files:
        return f'gs://sn_insights_test/{detected_files[0]}'
    raise ValueError('No file found in GCS bucket.')

with DAG('af_trig_df_dag_2',
         default_args=default_args,
         schedule_interval=None) as dag:

    # 1. Sensor to check for new files with a prefix (e.g., any .json file in cmark directory)
    file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_file_in_gcs',
        bucket='sn_insights_test',
        prefix='cmark/',  # Adjust this path as needed
        google_cloud_conn_id='google_cloud_default'
    )

    # 2. Python task to fetch detected file
    get_file_name = PythonOperator(
        task_id='get_file_name',
        python_callable=get_detected_file,
        provide_context=True
    )

    # 3. Trigger Dataflow Job using the detected file
    start_dataflow_job = DataflowTemplatedJobStartOperator(
    task_id='start_dataflow_job',
    template='gs://sn_insights_test/templates/cmark/cmark-template',
    job_name='cmark-df-job',
    location='europe-west2',
    gcp_conn_id='google_cloud_default',
)

    file_sensor >> get_file_name >> start_dataflow_job  # Set task dependencies