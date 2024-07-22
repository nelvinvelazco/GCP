from airflow import DAG
from google.cloud import storage
import os

from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'ETL_pyspark_gmaps',
    default_args=default_args,
    description='DAG que crea un clúster de Dataproc, ejecuta un job de PySpark y carga datos en BigQuery',
    schedule_interval=None,
)


# Configuración del clúster

REGION = 'southamerica-east1'
#PROJECT_ID = Variable.get('project_id')
#BUCKET_NAME = Variable.get('gcs_bucket')
#TEMP_BUCKET_NAME = Variable.get('gcs_temp_bucket')
#BQ_DATASET = Variable.get('bq_dataset')
#BQ_TABLE = Variable.get('bq_table')
CLUSTER_NAME = 'dataproc-cluster'
#REGION = 'us-central1'
PROJECT_ID = 'proyectohenry2'
BUCKET_NAME = 'data_proy'
PATH_FILES= 'google maps/metadata-sitios/'
TEMP_BUCKET_NAME = 'gmaps_data2'
BQ_DATASET = 'db_test'
BQ_TABLE = 'business'
#FOLDER_NAME= f'job_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
FOLDER_NAME= "job_20240715_210016/business"

# Define the job configuration
job_config = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {        
        "main_python_file_uri": f"gs://{TEMP_BUCKET_NAME}/job_ELT_gmaps.py",
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',                            
                            'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar'],        
        "args": [
            BUCKET_NAME,
            BQ_DATASET,
            BQ_TABLE,
            TEMP_BUCKET_NAME,
            PATH_FILES,
            FOLDER_NAME
        ]
    }
}

def list_gcs_files(bucket_name, prefix, **kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    kwargs['ti'].xcom_push(key='file_list', value=files)    

"""
# Task to create the cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,    
    region=REGION,
    cluster_name=CLUSTER_NAME,
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    dag=dag,
)

# Task to submit the job
submit_job = DataprocSubmitJobOperator(
    task_id='submit_dataproc_job',
    job=job_config,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)
"""

# Tarea para listar los archivos en GCS
list_files = PythonOperator(
    task_id='list_gcs_files',
    python_callable=list_gcs_files,
    op_kwargs={
        'bucket_name': TEMP_BUCKET_NAME,
        'prefix': FOLDER_NAME
    },
    provide_context=True,
    dag=dag,
)

# Tarea para cargar los archivos CSV en BigQuery
load_csv_to_bq = GCSToBigQueryOperator(
    task_id='load_csv_to_bq',
    bucket= TEMP_BUCKET_NAME,
    #source_objects=[f'{FOLDER_NAME}/*/*.csv'],
    source_objects="{{ task_instance.xcom_pull(task_ids='list_gcs_files', key='file_list') }}",
    destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.category',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    source_format='CSV',
    field_delimiter=',',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

# Task to delete the cluster
"""delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)   """

# Define task dependencies
list_files >> load_csv_to_bq
#create_cluster >> submit_job
#submit_job
#submit_job #>> delete_cluster
#submit_job #>> delete_cluster