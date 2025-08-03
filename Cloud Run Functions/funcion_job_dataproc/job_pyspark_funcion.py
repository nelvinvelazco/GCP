import sys
from datetime import datetime
from pyspark.sql import SparkSession

bucket_name = sys.argv[1]
gcs_ruta_file = sys.argv[2]
time = datetime.now().strftime("%Y%m%d_%H%M%S")

spark = SparkSession.builder.appName("procesar_csv").getOrCreate()

input_path = f"gs://{bucket_name}/{gcs_ruta_file}"
df = spark.read.csv(input_path, header=True)

# Ejemplo de transformación
df_transf = df.dropna()

# Guardar salida
output_path = f"gs://{bucket_name}/output/estados_usa-{time}.parquet"
df_transf.write.mode("overwrite").parquet(output_path)

print("Transformacion completa.")