from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
)

# Configuración del DAG
default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'Dataproc_Serverless',
    default_args=default_args,
    description='Envia y ejecuta un job de PySpark en Dataproc ServerLess y los guarda den unbucket',
    schedule_interval=None,
)

# Configuración del clúster
CLUSTER_NAME = 'dataproc-cluster'
#REGION = 'southamerica-east1'
REGION = 'us-central1'
#PROJECT_ID = Variable.get('project_id')
PROJECT_ID = 'practicas-432620'
#BUCKET_NAME = Variable.get('gcs_bucket')
BUCKET_DATA = 'data-pruebas'
BUCKET_PYSPARK_DATA = 'data_proc_proy'
#TEMP_BUCKET_NAME = Variable.get('gcs_temp_bucket')

# Define the job configuration
pyspark_batch = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{BUCKET_PYSPARK_DATA}/job_pyspark_reviews-yelp.py",
            "args": [
                f"gs://{BUCKET_DATA}/Yelp/review.json",
                BUCKET_PYSPARK_DATA
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