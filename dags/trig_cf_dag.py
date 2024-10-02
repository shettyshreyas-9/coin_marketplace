from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.utils.dates import days_ago
import os

# Setting up the Google Cloud service account credentials
try:
    SERVICE_ACCOUNT_KEY_PATH = '/opt/airflow/cred/gcp-test.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH
    print(f"Service account key path set to: {SERVICE_ACCOUNT_KEY_PATH}")
except Exception as e:
    print(f"An error occurred while setting the service account key path: {e}")


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow_SS',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='trigger_cloud_function_dag',
    default_args=default_args,
    description='A simple DAG to trigger a Cloud Function',
    schedule_interval=None,  # Set a schedule or trigger manually
) as dag:

    # Task to trigger Cloud Function
    trigger_cloud_function_1 = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_cloud_function_1',
        project_id='aceinternal-2ed449d3',
        location='europe-west2',
        input_data={},  # Pass event data if needed, else leave as an empty dictionary
        function_id='submissions-3',  # Cloud Function name
        gcp_conn_id='google_cloud_default',
    )

    trigger_cloud_function_2 = CloudFunctionInvokeFunctionOperator(
    task_id='trigger_cloud_function_2',
    project_id='aceinternal-2ed449d3',
    location='europe-west2',
    input_data={
        'name': 'etl_df_af/sample_test_19.csv', 'bucket': 'sn_insights_test'
    },
    function_id='grouping-2',  # Cloud Function name
    gcp_conn_id='google_cloud_default',
)



    # Define the task order
    trigger_cloud_function_1

    trigger_cloud_function_2
