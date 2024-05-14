import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io

def cargar_data(data):
    df_hechos= data.drop(['N_VICTIMAS', 'HORA', 'TIPO_DE_CALLE','VICTIMA', 
                                'ACUSADO', 'PARTICIPANTES'], axis=1)
    df_hechos= df_hechos[df_hechos['LONGITUD'] !='SIN DATO']
    df_hechos['FECHA'] = pd.to_datetime(df_hechos['FECHA'])
    #df_lesiones['longitud']= df_lesiones['longitud'].astype(float)
    #df_lesiones['latitud']= df_lesiones['latitud'].astype(float)
    return df_hechos

######################################################################
storage_client = storage.Client()

# Nombre del bucket y archivo CSV
bucket_name = 'gmaps_data2'
blob_name = 'sitios/hechos_homicidios.csv'

# Descargar el archivo CSV desde GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
content = blob.download_as_string()

# Crear un DataFrame de Pandas a partir del contenido del archivo CSV
df = pd.read_csv(io.BytesIO(content))
df= cargar_data(df)

#########################################################

bigquery_client = bigquery.Client()

#project_id = 'tu-proyecto-id'
dataset_id = 'BD_Henry'
table_id = 'hechos_homicidios'
schema = [
        bigquery.SchemaField("Id_hecho", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("FECHA", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("COMUNA", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("LONGITUD", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("LATITUD", bigquery.enums.SqlTypeNames.STRING)
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
print('Registros agregados correctamente.')