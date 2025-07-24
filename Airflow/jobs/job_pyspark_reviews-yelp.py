import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, udf, lit, trim, explode
from pyspark.sql.types import StringType, StructType, StructField

# Argumentos
gcs_input_path = sys.argv[1]
#gcs_input_path = 'gs://data-pruebas/google maps/metadata-sitios/1.json'
bq_dataset = sys.argv[2]
bq_table = sys.argv[3]
temporary_gcs_bucket = sys.argv[4]
gcs_estados = 'gs://data_proc_proy/estados_usa.csv'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('ELT y BigQuery') \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temporary_gcs_bucket)

# Leer datos desde GCS
df_sitios = spark.read.json(gcs_input_path)
df_estados = spark.read.option("delimiter", ";").option("header", "true").csv(gcs_estados)

# Convertir el DataFrame a Pandas y luego a un diccionario
estados_dict = df_estados.select("nombre_corto", "nombre_largo").toPandas().set_index("nombre_corto").to_dict()["nombre_largo"]
