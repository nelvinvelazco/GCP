from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Función para imprimir los nombres de los archivos
def print_file_names(**kwargs):
    file_names = kwargs['ti'].xcom_pull(task_ids='list_files')
    for name in file_names:
        print(name)

# Define el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'list_files_in_gcs_bucket',
    default_args=default_args,
    description='Un DAG para listar archivos en un bucket de GCS',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarea para listar archivos en la carpeta del bucket
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket='data_proy',
        prefix='google maps/metadata-sitios/',  # Asegúrate de que termine con '/'        
    )

    # Tarea para imprimir los nombres de los archivos
    print_files = PythonOperator(
        task_id='print_files',
        python_callable=print_file_names,
        provide_context=True,
    )

    list_files >> print_files