from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Define default arguments
default_args = {
    'owner': 'airflow_SS',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'af_trig_df_dag_3',
    default_args=default_args,
    schedule_interval=None,  # Triggered by Pub/Sub
    catchup=False,
) as dag:
    

    # Define the Pub/Sub sensor to listen for new messages
    wait_for_gcs_file = PubSubPullSensor(
        task_id='wait_for_gcs_file',
        project_id='aceinternal-2ed449d3',
        subscription='gcs-file-upd-sub',  # Subscription for the Pub/Sub topic
        max_messages=1,
        timeout=900,
        # timeout=timedelta(days=7).total_seconds(),  # Wait indefinitely for new messages
        ack_messages=True,
    )



    # Define the Dataflow job template operator
    run_dataflow_job = DataflowTemplatedJobStartOperator(
        task_id='run_dataflow_job',
        template='gs://sn_insights_test/templates/cmark/cmark-template',
        # job_id='dataflow-job-{{ ds }}',  # Unique job ID
        # parameters={'param1': 'value1', 'param2': 'value2'},  # Replace with your parameters
        location='europe-west2',  # Specify your region
    )


    # Trigger this DAG to rerun itself
    trigger_next_run = TriggerDagRunOperator(
        task_id='trigger_next_run',
        trigger_dag_id='af_trig_df_dag_3',  # This will trigger itself
        wait_for_completion=False,  # Do not wait for the triggered DAG to complete
    )

    

    # Set task dependencies
    wait_for_gcs_file >> trigger_next_run >> run_dataflow_job
