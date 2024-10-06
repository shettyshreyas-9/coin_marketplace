from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator


from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow_SS',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define DAG
with DAG(
    'af_trig_df_dag_1',
    default_args=default_args,
    schedule_interval=None,  # Triggered by GCS event or manually
    catchup=False
) as dag:

    # 1. Sensor to check for new files in the GCS bucket
    check_gcs_file = GCSObjectUpdateSensor(
        task_id='check_gcs_file',
        bucket='sn_insights_test',
        object='cmark/',  # Use a wildcard to detect any file in the 'cmark' folder
        mode='poke',  # Can also use 'reschedule' for more efficient polling
        google_cloud_conn_id='google_cloud_default',
        timeout=600,   # Set timeout period for the sensor
        poke_interval=10  # How often to check for the file
    )

    # 2. Trigger Dataflow Job using a pre-existing Dataflow template
    start_dataflow_job = DataflowTemplatedJobStartOperator(
        task_id='start_dataflow_job',
        project_id='aceinternal-2ed449d3',
        template='gs://sn_insights_test/templates/cmark/cmark-template',  # Path to Dataflow template
        location='europe-west2',  # Region of the Dataflow job
        gcp_conn_id='google_cloud_default',
        # wait_until_finished=False
    )

    # DAG sequence: wait for file -> trigger Dataflow job
    check_gcs_file >> start_dataflow_job