import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io

def cargar_data(data):
    df_sitios= data.drop(['relative_results','address', 'num_of_reviews', 'description', 'url','category', 'MISC', 'hours'], axis=1) 
    df_sitios= df_sitios.dropna(subset=['name']) # Elimina filas con address nulas
    df_sitios['name']= df_sitios['name'].astype('string')
    # Ordena el orden de las columnas
    df_sitios= df_sitios[['gmap_id','name', 'latitude', 'longitude', 'avg_rating', 'price', 'state']]
    return df_sitios

######################################################################
storage_client = storage.Client()

# Nombre del bucket y archivo CSV
bucket_name = 'gmaps_data2'
blob_name = 'sitios/1.json'

# Descargar el archivo CSV desde GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
content = blob.download_as_string()

# Crear un DataFrame de Pandas a partir del contenido del archivo CSV
df = pd.read_json(io.BytesIO(content),lines=True)
df= cargar_data(df)
print("ARCHIVO CARGADO Y TRANFORMADO DESDE EL BUCKET")

#########################################################

bigquery_client = bigquery.Client()

#project_id = 'tu-proyecto-id'
dataset_id = 'BD_Henry'
table_id = 'sitios_gmaps'
schema = [
        bigquery.SchemaField("gmap_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("latitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("longitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("avg_rating", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("state", bigquery.enums.SqlTypeNames.STRING),
    ]
table_ref = bigquery_client.dataset(dataset_id).table(table_id)
try:
    tabla = bigquery_client.get_table(table_ref)
    tabla_existe = True
except:
    tabla_existe = False

if tabla_existe:
    print(f'La tabla {table_id} ya existe en el dataset {dataset_id}.')
else:
    print(f'La tabla {table_id} no existe en el dataset {dataset_id}.')
    # Crear la tabla si no existe
    tabla = bigquery.Table(table_ref, schema=schema)
    tabla = bigquery_client.create_table(tabla)
    print(f'Se ha creado la tabla {table_id} en el dataset {dataset_id}.')


#df.to_gbq(table_ref,if_exists="replace")
# Agregar los registros de df a la tabla existente o recién creada
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if tabla_existe else bigquery.WriteDisposition.WRITE_TRUNCATE
job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()
print('REGISTROS AGREGADOS CORRECTAMENTE.')