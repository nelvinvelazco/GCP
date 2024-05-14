import pandas as pd
from google.cloud import bigquery
from google.cloud import storage


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')



bucket_name = 'gmaps_data'
source_file_name = 'data/hechos_lesiones.csv'
destination_blob_name = 'sitios/hechos_lesiones.csv'

# Upload the CSV file to GCS
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)