from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import pandas as pd
import logging
import io
import os

# Configuración del DAG
default_args = {
    'owner': 'Nelvin Velazco',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='Test_GCP',
    default_args=default_args,
    description='Prueba del uso de la libreria python',
    schedule_interval=None,  # Ejecutar manualmente
    catchup=False,
)

# Función para leer el archivo desde GCS
def leer_archivo_gcp(**kwargs):
    try:
        client = storage.Client()
        bucket_name = 'gmaps_data2'
        source_blob_name = '1.json'

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        content = blob.download_as_string()
        df = pd.read_json(io.StringIO(content.decode('utf-8')),lines=True)
        df.head()
        #kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    except Exception as e:
        logging.error(f"Error in la funcion: {e}")
        raise

# Definición de las tareas en el DAG
pandas_task = PythonOperator(
    task_id='test_gcp_airflow',
    python_callable=leer_archivo_gcp,
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de tareas
pandas_task 