from datetime import datetime, timedelta
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
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='gcs_read_process_write',
    default_args=default_args,
    description='Read file from GCS, process with Python, and save back to GCS',
    schedule_interval=None,  # Ejecutar manualmente
    catchup=False,
)

# Función para leer el archivo desde GCS
def read_file_from_gcs(**kwargs):
    try:
        client = storage.Client()
        bucket_name = 'gmaps_data2'
        source_blob_name = '1.json'

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        content = blob.download_as_string()
        df = pd.read_json(io.StringIO(content.decode('utf-8')),lines=True)
    
        # Guardar el DataFrame en XCom para pasarlo a la siguiente tarea
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    except Exception as e:
        logging.error(f"Error in read_file_from_gcs: {e}")
        raise

# Función para procesar los datos
def process_data(**kwargs):
    # Obtener el DataFrame desde XCom
    raw_data_json = kwargs['ti'].xcom_pull(key='raw_data', task_ids='read_file_from_gcs')
    df = pd.read_json(raw_data_json, lines=True)
    
    # Realizar transformaciones en el DataFrame
    df['Es_Restaurant'] = df['category'].apply(lambda x: 'Restaurant' in x)
    df= df[df['Es_Restaurant']]
    df= df.drop(['relative_results','address', 'num_of_reviews', 'description', 'url', 'MISC', 'hours'], axis=1)

    # Guardar el DataFrame procesado en XCom para pasarlo a la siguiente tarea
    kwargs['ti'].xcom_push(key='processed_data', value=df.to_json())

# Función para guardar el archivo procesado en GCS
def write_file_to_gcs(**kwargs):
    client = storage.Client()
    bucket_name = 'gmaps_data2'
    destination_blob_name = f'processed_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    
    # Obtener el DataFrame procesado desde XCom
    processed_data_json = kwargs['ti'].xcom_pull(key='processed_data', task_ids='process_data')
    df = pd.read_json(processed_data_json)
    
    # Guardar el DataFrame en un archivo temporal
    temp_file_path = 'tmp/processed_data.csv'
    df.to_csv(temp_file_path, index=False)
    
    # Subir el archivo procesado a GCS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(temp_file_path)
    
    # Eliminar el archivo temporal
    os.remove(temp_file_path)

# Definición de las tareas en el DAG
read_task = PythonOperator(
    task_id='read_file_from_gcs',
    python_callable=read_file_from_gcs,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_file_to_gcs',
    python_callable=write_file_to_gcs,
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de tareas
read_task >> process_task >> write_task