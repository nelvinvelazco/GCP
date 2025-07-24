from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
)

from airflow.models import Variable

# Configuración del DAG
default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'Dataproc_Serverless',
    default_args=default_args,
    description='Envia y ejecuta un job de PySpark en Dataproc ServerLess y carga datos en BigQuery',
    schedule_interval=None,
)

# Configuración del clúster
CLUSTER_NAME = 'dataproc-cluster'
#REGION = 'southamerica-east1'
REGION = 'us-central1'
#PROJECT_ID = Variable.get('project_id')
PROJECT_ID = 'practicas-432620'
#BUCKET_NAME = Variable.get('gcs_bucket')
BUCKET_NAME = 'data-pruebas'
TEMP_BUCKET_NAME = 'data_proc_proy'
#TEMP_BUCKET_NAME = Variable.get('gcs_temp_bucket')
#BQ_DATASET = Variable.get('bq_dataset')
BQ_DATASET = 'db_test'
#BQ_TABLE = Variable.get('bq_table')
BQ_TABLE = 'business'

# Define the job configuration
pyspark_batch = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{TEMP_BUCKET_NAME}/pyspark_job_gmaps.py",
            "args": [
                f"gs://{BUCKET_NAME}/google maps/metadata-sitios/1.json",
                BQ_DATASET,
                BQ_TABLE,
                TEMP_BUCKET_NAME
            ]
        },
        "runtime_config": {
            "version": "2.1"
        },
        "labels": {"env": "test"},
}


# Task to submit the job
enviar_job = DataprocCreateBatchOperator(
    task_id='enviar_dataproc_serverless_job',
    region=REGION,
    project_id=PROJECT_ID,
    batch=pyspark_batch,
    batch_id= "serverlessjob-{{ execution_date.strftime('%Y%m%d-%H%M%S') }}",
    dag= dag,
)


# Define task dependencies
enviar_job
#enviar_job #>> delete_cluster