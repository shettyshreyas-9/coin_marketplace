from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import time
from google.cloud import storage

# CoinMarketCap API details
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'5000',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': 'b7f1b855-e86f-44e9-85c7-0bb742c95de3'
}

# Create a session
session = Session()
session.headers.update(headers)

# Google Cloud Storage Bucket details
bucket_name = 'sn_insights_test'
folder_name = 'cmark'

# Function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, folder_name, file_name, data):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{file_name}")
    blob.upload_from_string(data, content_type='application/json')
    print(f"Data uploaded to {bucket_name}/{folder_name}/{file_name}")

try:
    # Fetch data from the CoinMarketCap API
    response = session.get(url, params=parameters)
    data = json.dumps(response.json(), indent=2)  # Pretty print JSON data

    # Create a unique filename using timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"coin_data_{timestamp}.json"

    # Upload the data to the GCS bucket
    upload_to_gcs(bucket_name, folder_name, file_name, data)

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
