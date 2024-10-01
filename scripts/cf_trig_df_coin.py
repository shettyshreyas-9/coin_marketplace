# requiremnts
# google-api-python-client
# google-auth
# google-cloud-storage

import google.auth
from googleapiclient.discovery import build

def trigger_dataflow(data, context):
    # Get the bucket name and file name from the event
    bucket_name = data['bucket']
    file_name = data['name']

    # Only trigger the Dataflow job if the file is uploaded to the 'gs://sn_insights_test/etl_df_af' bucket
    target_bucket = 'sn_insights_test'
    target_prefix = 'cmark/'

    if bucket_name == target_bucket and file_name.startswith(target_prefix):
        project = 'aceinternal-2ed449d3'
        template = 'gs://sn_insights_test/templates/cmark/cmark-template'
        location = 'europe-west2'
        
        # Constructing the Dataflow request
        credentials, _ = google.auth.default()
        service = build('dataflow', 'v1b3', credentials=credentials)

        request_body = {
            "jobName": "cmark-dataflow-job",
            "environment": {
                "tempLocation": "gs://sn_insights_test/temp"
            }
        }

        # Launch the Dataflow job
        request = service.projects().locations().templates().launch(
            projectId=project,
            location=location,
            gcsPath=template,
            body=request_body
        )
        
        response = request.execute()
        print(f"Dataflow job triggered: {response}")
    else:
        print(f"No action taken. File uploaded to {bucket_name}/{file_name}, which is outside the target path.")
