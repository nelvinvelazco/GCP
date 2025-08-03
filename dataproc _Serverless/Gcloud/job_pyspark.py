import sys
from datetime import datetime
from pyspark.sql import SparkSession

bucket_name = 'data-pruebas'
file_name =  "job_pyspark.py"
bucket_destino= 'data_proc_proy' 
time = datetime.now().strftime("%Y%m%d_%H%M%S")

spark = SparkSession.builder.appName("procesar_csv") \
    .config("spark.jars","gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

input_path = f"gs://{bucket_name}/{file_name}"
df = spark.read.csv(input_path, header=True)

# Ejemplo de transformación
df_transf = df.dropna()

# Guardar salida
output_path = f"gs://{bucket_destino}/output/estados_usa-{time}.parquet"
df_transf.write.mode("overwrite").parquet(output_path)

print("Transformacion completa.")