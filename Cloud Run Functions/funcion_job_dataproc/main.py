import functions_framework
import os
from datetime import datetime
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import (
    Batch,
    PySparkBatch,
)

@functions_framework.cloud_event
def funcion_dataproc(cloud_event):
    data = cloud_event.data

    bucket = data.get("bucket")
    name = data.get("name")
    time = datetime.now().strftime("%Y%m%d-%H%M%S")
    #file_name= f"gs://{bucket}/{name}"  # hay que colocar gcsfs en requirements.txt para que lea archvos de GCS
    #df_estados= pd.read_csv(file_name, delimiter=";")
    #print(df_estados.head())

    print(f"--------- Archivo subido: {name} en el bucket: {bucket} --------------")

    # Parámetros
    region = "us-central1"
    project_id = "practicas-432620"
    spark_file_gcs = "gs://data_proc_proy/job_pyspark_funcion.py" 

    # Crear cliente
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Definir el job
    batch = Batch(
        pyspark_batch=PySparkBatch(
            main_python_file_uri=spark_file_gcs,
            args=[bucket, name]
        ),
        runtime_config={"version": "1.1"},        
    )

    # Ejecutar en Serverless
    parent = f"projects/{project_id}/locations/{region}"

    response = client.create_batch(
        request={"parent": parent, "batch": batch, "batch_id": f"batch-test-{time}"}
    )

    print("---- JOB ENVIADO CORRECTAMENTE ---------")