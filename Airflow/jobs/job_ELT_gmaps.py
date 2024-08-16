import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, udf, lit, trim, explode
from pyspark.sql.types import StringType, StructType, StructField
from google.cloud import storage

# Argumentos
bucket_name = sys.argv[1]
bq_dataset = sys.argv[2]
bq_table = sys.argv[3]
tmp_gcs_bucket = sys.argv[4]
files_path = sys.argv[5]
folder_name= sys.argv[6]
gcs_estados = 'gs://data_proc_proy/estados_usa.csv'
cont= 0

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName('GCS to BigQuery') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', tmp_gcs_bucket)

client = storage.Client()
bucket = client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix=files_path)
file_list = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

for file in file_list:
    # Leer datos desde GCS
    cont=cont+1    
    print(f'PROCESANDO {file} ....')
    df_sitios = spark.read.json(file)
    df_estados = spark.read.option("delimiter", ";").option("header", "true").csv(gcs_estados)

    # Convertir el DataFrame a Pandas y luego a un diccionario
    estados_dict = df_estados.select("nombre_corto", "nombre_largo").toPandas().set_index("nombre_corto").to_dict()["nombre_largo"]

    df_sitios = df_sitios.drop('relative_results', 'num_of_reviews', 'description', 'url', 'hours')
    df_sitios = df_sitios.dropDuplicates(['gmap_id'])
    df_sitios = df_sitios.dropna(subset=['address'])
    df_sitios = df_sitios.dropna(subset=['category'])

    # Crear una nueva columna que indique si la categoría contiene 'Restaurant'
    df_sitios = df_sitios.withColumn('Es_Restaurant', array_contains(col('category'), 'Restaurant'))
    df_sitios = df_sitios.filter(col('Es_Restaurant') == True)

    df_sitios= df_sitios.fillna({'price':'SIN DATO', 'state':'SIN DATO'})     # Se imputan los valores nulos a 'SIN DATO'

    # Definir la función UDF
    def ext_ciudad_estado(dir, estados_dict):
        ciudad = "SIN DATO"
        estado = "SIN DATO"
        if len(str(dir)) > 10:
            lista = str(dir).split(',')
            if len(lista) > 2:
                codigo = lista[-1][1:3]
                estado = estados_dict.get(codigo, "SIN DATO")
                ciudad = lista[-2].strip() if estado != "SIN DATO" else "SIN DATO"
        return ciudad, estado

    # Registrar la UDF usando el diccionario
    @udf(StructType([StructField("ciudad", StringType(), True), StructField("estado", StringType(), True)]))
    def ext_ciudad_estado_udf(dir):
        return ext_ciudad_estado(dir, estados_dict)

    # Aplicar la UDF al DataFrame
    df_sitios = df_sitios.withColumn("ciudad_estado", ext_ciudad_estado_udf(df_sitios["address"]))
    df_sitios = df_sitios.withColumn("ciudad", col("ciudad_estado").getItem("ciudad")) \
                        .withColumn("estado", col("ciudad_estado").getItem("estado")) \
                        .drop("ciudad_estado")

    df_sitios = df_sitios.withColumn('estado', trim(col('estado')))
    lista_estados= ['Florida', 'Pennsylvania', 'Tennessee', 'California', 'Texas', 'New York']

    # Filtrar el DataFrame
    df_sitios = df_sitios.filter(col('estado').isin(lista_estados))

    # Seleccionar las columnas necesarias
    df_category = df_sitios.select('gmap_id', 'category')
    # Explotar la columna 'category'
    df_category = df_category.withColumn('category', explode(col('category')))
    df_category = df_category.withColumnRenamed('gmap_id', 'business_id') \
                            .withColumnRenamed('category', 'category_name')

    # Seleccionar las columnas necesarias
    df_misc = df_sitios.select('gmap_id', 'MISC')
    # Expandir el struct a columnas individuales
    df_misc = df_misc.select('gmap_id', 'MISC.*')

    # Seleccionar las columnas necesarias
    df_Service_options = df_misc.select('gmap_id', 'Service options')
    # Explotar la columna 'Service options'
    df_Service_options = df_Service_options.withColumn('Service options', explode(col('Service options')))
    df_Service_options = df_Service_options.withColumnRenamed('gmap_id', 'business_id') \
                                            .withColumnRenamed('Service options', 'service_options')

    # Seleccionar las columnas necesarias
    df_Planning = df_misc.select('gmap_id', 'Planning')
    # Explotar la columna 'category'
    df_Planning = df_Planning.withColumn('Planning', explode(col('Planning')))
    df_Planning = df_Planning.withColumnRenamed('gmap_id', 'business_id') \
                            .withColumnRenamed('Planning', 'planning_options')

    df_sitios = df_sitios.drop('MISC', 'address', 'category', 'state', 'Es_Restaurant')
    df_sitios = df_sitios.withColumnRenamed('ciudad', 'city') \
                            .withColumnRenamed('estado', 'state') \
                            .withColumnRenamed('gmap_id', 'business_id') \
                            .withColumnRenamed('avg_rating', 'stars')

    df_sitios = df_sitios.withColumn("platform", lit(1))
    df_sitios = df_sitios.select(['business_id','name', 'city', 'state', 'latitude', 'longitude', 'stars', 'price','platform'])
    
    
    #df_category.show()
    #df_Service_options.show()
    #df_Planning.show()
    #df_sitios.show()

    df_category= df_category.coalesce(1)
    df_category.write.format('csv') \
                .mode('overwrite') \
                .option('header', 'true') \
                .save(f"gs://{tmp_gcs_bucket}/{folder_name}/category/category_business_{cont}.csv")

    df_Service_options= df_Service_options.coalesce(1)
    df_Service_options.write.format('csv') \
                .mode('overwrite') \
                .option('header', 'true') \
                .save(f"gs://{tmp_gcs_bucket}/{folder_name}/service/service_business_{cont}.csv")

    df_Planning= df_Planning.coalesce(1)
    df_Planning.write.format('csv') \
                .mode('overwrite') \
                .option('header', 'true') \
                .save(f"gs://{tmp_gcs_bucket}/{folder_name}/planning/planning_business_{cont}.csv")
    
    df_sitios= df_sitios.coalesce(1)
    df_sitios.write.format('csv') \
                .mode('overwrite') \
                .option('header', 'true') \
                .save(f"gs://{tmp_gcs_bucket}/{folder_name}/business/business_{cont}.csv")

    print(f"---- ARCHIVO {file} PROCESADO ----")

