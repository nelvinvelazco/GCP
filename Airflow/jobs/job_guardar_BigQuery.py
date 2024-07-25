import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, udf, lit, trim, explode
from pyspark.sql.types import StringType, StructType, StructField
from google.cloud import storage

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('GCS to BigQuery') \
    .getOrCreate()