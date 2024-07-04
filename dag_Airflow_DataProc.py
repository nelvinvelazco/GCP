from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.models import Variable

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'dataproc_gcs_to_bq',
    default_args=default_args,
    description='DAG que crea un clúster de Dataproc, ejecuta un job de PySpark y carga datos en BigQuery',
    schedule_interval=None,
)

# Configuración del clúster
CLUSTER_NAME = 'dataproc-cluster'
REGION = 'southamerica-east1'
PROJECT_ID = Variable.get('project_id')
BUCKET_NAME = Variable.get('gcs_bucket')
TEMP_BUCKET_NAME = 'gmaps_data2'
#TEMP_BUCKET_NAME = Variable.get('gcs_temp_bucket')
BQ_DATASET = Variable.get('bq_dataset')
#BQ_TABLE = Variable.get('bq_table')
BQ_TABLE = 'estados_usa2'

# Define the cluster configuration
"""cluster_config = ClusterGenerator(
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    num_workers=2,
    region=REGION,
    init_actions_uris=["gs://goog-dataproc-initialization-actions-southamerica-east1/python/pip-install.sh"],
    metadata={'PIP_PACKAGES': 'google-cloud-storage'}
).make()"""

# Define the job configuration
job_config = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {        
        "main_python_file_uri": f"gs://{BUCKET_NAME}/pyspark_job.py",
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',                            
                            'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar'],
        "args": [
            f"gs://{BUCKET_NAME}/estados_usa.csv",
            BQ_DATASET,
            BQ_TABLE,
            TEMP_BUCKET_NAME
        ]
    }
}

# Task to create the cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    #cluster_config=cluster_config,
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

# Task to delete the cluster
"""delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)   """

# Define task dependencies
#create_cluster >> submit_job >> delete_cluster
submit_job #>> delete_cluster