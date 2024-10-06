
# from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration, DataflowTemplatedJobStartOperator , DataflowStartFlexTemplateOperator

# # Check attributes of DataflowConfiguration
# print(dir(DataflowTemplatedJobStartOperator))

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
# Check attributes of DataflowConfiguration
print(dir(GCSListObjectsOperator))