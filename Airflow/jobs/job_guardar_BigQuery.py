import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, udf, lit, trim, explode
from pyspark.sql.types import StringType, StructType, StructField
from google.cloud import storage

bq_dataset = sys.argv[1]
tmp_gcs_bucket = sys.argv[2]
folder_name= sys.argv[3]
tables_list= ['category', 'service', 'planning', 'business']

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('GCS to BigQuery') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', tmp_gcs_bucket)

storage_client = storage.Client()
bucket = storage_client.bucket(tmp_gcs_bucket)

for tabla in tables_list:
    dir= f"{folder_name}/{tabla}/"
    print(f'Ruta - {dir}') 
    blobs = bucket.list_blobs(prefix= dir)
    file_list = [f"gs://{tmp_gcs_bucket}/{blob.name}" for blob in blobs if blob.name.endswith('.csv')]
    for file in file_list:
        df_etl = spark.read.csv(file, header=True, inferSchema=True)
        # Escribir datos en BigQuery
        df_etl.write.format('bigquery') \
            .option('table',f'{bq_dataset}.{tabla}') \
            .option('temporaryGcsBucket', tmp_gcs_bucket) \
            .option('createDisposition', 'CREATE_IF_NEEDED') \
            .option('writeDisposition', 'WRITE_TRUNCATE') \
            .mode('append') \
            .save()
        print(f'TABLA "{tabla}-{file}" GUARDADA EN BIGQUERY')
