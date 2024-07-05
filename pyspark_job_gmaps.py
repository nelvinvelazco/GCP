import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, whenfrom, array_contains

# Argumentos
gcs_input_path = sys.argv[1]
bq_dataset = sys.argv[2]
bq_table = sys.argv[3]
temporary_gcs_bucket = sys.argv[4]

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('GCS to BigQuery') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temporary_gcs_bucket)

# Leer datos desde GCS
df_data = spark.read.json(gcs_input_path, multiLine=True)
df_data = df_data.dropDuplicates(['gmap_id'])
df_data = df_data.dropna(subset=['address'])
df_data = df_data.dropna(subset=['category'])
df_data = df_data.withColumn('Es_Restaurant', array_contains(col('category'), 'Restaurant'))
df_data = df_data.filter(col('Es_Restaurant') == True)
df_data = df_data.fillna({'price': 'SIN DATO', 'state': 'SIN DATO'})
df_data = df_data.drop('relative_results', 'address', 'num_of_reviews', 'description', 'url', 'category', 'MISC', 'hours')

df_data.printSchema()
df_data.show()


# Escribir datos en BigQuery
"""
df_data.write.format('bigquery') \
    .option('table',f'{bq_dataset}.{bq_table}') \
    .option('temporaryGcsBucket', temporary_gcs_bucket) \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .option('writeDisposition', 'WRITE_TRUNCATE') \
    .mode('append') \
    .save()
"""
spark.stop()