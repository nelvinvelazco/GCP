import sys
from pyspark.sql import SparkSession

# Argumentos
gcs_input_path = sys.argv[1]
bq_dataset = sys.argv[2]
bq_table = sys.argv[3]
temporary_gcs_bucket = sys.argv[4]

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('GCS to BigQuery') \
    .getOrCreate()

""" .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.39.1") \ """

spark.conf.set('temporaryGcsBucket', temporary_gcs_bucket)

# Leer datos desde GCS
df = spark.read.csv(gcs_input_path, header=True, inferSchema=True, sep=';')

df.printSchema()
df.show()


# Escribir datos en BigQuery

df.write.format('bigquery') \
    .option('table',f'{bq_dataset}.{bq_table}') \
    .option('temporaryGcsBucket', temporary_gcs_bucket) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .option('writeDisposition', 'WRITE_TRUNCATE') \
    .save()
    
    #.mode('append') \
    #.option('createDisposition', 'CREATE_IF_NEEDED') \
    #.option('writeDisposition', 'WRITE_TRUNCATE') \

spark.stop()