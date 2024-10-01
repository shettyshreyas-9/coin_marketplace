import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.gcp.internal.clients import bigquery
import datetime
import os  # Import os module to handle file paths

import apache_beam.io.fileio as fo  # had to use this as normal import was giving error

class ParseJson(beam.DoFn):
    def process(self, element):
        file_name, content = element
        
        import datetime  # this is very imp. for df worker to run job successfully

        try:
            # Extract only the base filename
            base_file_name = os.path.basename(file_name)
            timestamp = datetime.datetime.utcnow().isoformat() + 'Z'

            print(f"Processing file: {base_file_name}")  # Log the base file name
            record = json.loads(content)
            for coin in record['data']:
                yield {
                    'id': coin['id'],
                    'name': coin['name'],
                    'symbol': coin['symbol'],
                    'quote': {
                        'USD': {
                            'price_usd': coin['quote']['USD']['price'],
                            'market_cap': coin['quote']['USD']['market_cap'],
                            'percent_change_24h': coin['quote']['USD']['percent_change_24h']
                        }
                    },
                    'input_file': base_file_name,  # Use the base file name
                    'timestamp': timestamp 
                    
                }
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in {file_name}: {e}")
        except Exception as e:
            print(f"Unexpected error in {file_name}: {e}")
        finally:
            print(f"Yields from file {base_file_name}: {coin['id']}")

def get_processed_files(output_table):
    # Create a BigQuery client to retrieve already processed file names
    from google.cloud import bigquery

    # Explicitly specify the project ID when initializing the client
    client = bigquery.Client(project='aceinternal-2ed449d3')

    # Make sure to use the fully qualified table name
    query = f"""
        SELECT DISTINCT input_file
        FROM `aceinternal-2ed449d3.sandbox_ss_eu.cmark`
    """
    query_job = client.query(query)
    results = query_job.result()

    # Store processed file names in a set for faster lookup
    processed_files = {row.input_file for row in results}
    return processed_files


def run(argv=None):
    # Input and output locations
    input_file = 'gs://sn_insights_test/cmark/coin_data_*.json'  # You can use * to match multiple files
    output_table = 'aceinternal-2ed449d3.sandbox_ss_eu.cmark'  # BigQuery table

    # Pipeline options
    pipeline_options = PipelineOptions(
        project='aceinternal-2ed449d3',
        job_name='cmark-etl-1',
        region='europe-west2',
        temp_location='gs://sn_insights_test/temp',
        staging_location='gs://sn_insights_test/staging',
        runner='DataflowRunner'  # Uncomment this if using Dataflow runner
    )


    # Define BigQuery schema programmatically with nested fields
    def get_bq_schema():
        fields = []

        # Main fields
        fields.append(bigquery.TableFieldSchema(name='id', type='INTEGER'))
        fields.append(bigquery.TableFieldSchema(name='name', type='STRING'))
        fields.append(bigquery.TableFieldSchema(name='symbol', type='STRING'))

        # Nested USD field under 'quote'
        usd_fields = [
            bigquery.TableFieldSchema(name='price_usd', type='FLOAT'),
            bigquery.TableFieldSchema(name='market_cap', type='FLOAT'),
            bigquery.TableFieldSchema(name='percent_change_24h', type='FLOAT')
        ]

        quote_usd_schema = bigquery.TableFieldSchema(name='USD', type='RECORD', fields=usd_fields)
        quote_schema = bigquery.TableFieldSchema(name='quote', type='RECORD', fields=[quote_usd_schema])

        fields.append(quote_schema)

        # Add fields for input file and timestamp
        fields.append(bigquery.TableFieldSchema(name='input_file', type='STRING'))
        fields.append(bigquery.TableFieldSchema(name='timestamp', type='TIMESTAMP'))

        return bigquery.TableSchema(fields=fields)


    # Get already processed files from BigQuery
    processed_files = get_processed_files(output_table)

    # Create and run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'MatchFiles' >> fo.MatchFiles(input_file)  # Match files based on the pattern
            | 'ReadMatches' >> beam.io.fileio.ReadMatches()          # Read matched files
            | 'ReadFileContents' >> beam.Map(lambda x: (x.metadata.path, x.read()))  # Read the content of each file
            | 'FilterNewFiles' >> beam.Filter(lambda x: os.path.basename(x[0]) not in processed_files)  # Filter new files
            | 'ParseJSON' >> beam.ParDo(ParseJson())   # No need to pass file name directly here; it is included in input
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema=get_bq_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
