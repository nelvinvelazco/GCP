import pandas as pd
from google.cloud import storage
import os


def subir_to_gcp(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')


ruta_archivos = "C:/Data/GMaps/"     # Ruta de los json de Sitios
archivos_json = [[os.path.join(ruta_archivos, archivo), archivo] for archivo in os.listdir(ruta_archivos) if archivo.endswith('.json')]
bucket_name = 'gmaps_data2'

lista_df_sitios = []
for archivo in archivos_json:           
    source_file_name = archivo[0]
    destination_blob_name = archivo[1]
    # Upload the CSV file to GCS
    subir_to_gcp(bucket_name, source_file_name, destination_blob_name)
    print(f"SE SUBIO EL ARCHIVO: {destination_blob_name}")