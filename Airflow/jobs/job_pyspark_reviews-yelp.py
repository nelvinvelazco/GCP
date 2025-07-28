import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, udf, lit, trim, explode, isnan
from pyspark.sql.types import StringType, StructType, StructField
import pickle
import pandas as pd
from datetime import datetime

# Argumentos
gcs_ruta_file = sys.argv[1]
bucket_pyspark = sys.argv[2]
gcs_estados = 'gs://data_proc_proy/estados_usa.csv'
gcs_business_yelp = 'gs://data-pruebas/Yelp/business.pkl'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('ELT y BigQuery') \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()


# Leer datos desde GCS
df_estados = spark.read.option("delimiter", ";").option("header", "true").csv(gcs_estados)
df_estados= df_estados.withColumnRenamed("nombre_largo","estado")
df_estados= df_estados.withColumnRenamed("nombre_corto","state")
df_estados= df_estados.drop("codigos")
df_estados.show(3)

df = pd.read_pickle(gcs_business_yelp)
df= df.loc[:, ~df.columns.duplicated()]
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["stars"] = pd.to_numeric(df["stars"], errors="coerce")
df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce")
df["is_open"] = pd.to_numeric(df["is_open"], errors="coerce")

df_business = spark.createDataFrame(df)
df_business.show(3)
df_business = df_business.dropDuplicates(['business_id'])
df_business = df_business.withColumn("Es_Restaurant", col("categories").contains("Restaurant"))
df_business = df_business.filter(col('Es_Restaurant') == True)
df_business = df_business.filter(~(col("state").isNull() | isnan(col("state"))))
df_business = df_business.join(df_estados, on="state", how="left")
df_business= df_business.drop('state')
df_business = df_business.withColumn('estado', trim(col('estado')))
lista_estados= ['Florida', 'Pennsylvania', 'Tennessee', 'California', 'Texas', 'New York']
df_business = df_business.filter(col('estado').isin(lista_estados))
df_business= df_business.select('business_id', 'name')
df_business.show(5)

df_reviews = spark.read.json(gcs_ruta_file)
df_reviews.show(3)
df_reviews= df_reviews.join(df_business, on="business_id",how='inner')
df_reviews.show(3)

df_reviews.count()
df_reviews.select('business_id').distinct().count()
time = datetime.now().strftime("%Y%m%d_%H%M%S")

df_reviews.write.mode('overwrite')\
                .parquet(f"gs://{bucket_pyspark}/reviews_yelp-{time}")

print(f"---- ARCHIVO {gcs_ruta_file} PROCESADO ----")