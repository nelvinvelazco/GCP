import pandas as pd
from google.cloud import bigquery
import datetime
import io

def check_reg_existe_en_bigQ(row, table_id, client):
    query = f"""
    SELECT COUNT(*)
    FROM `{table_id}`
    WHERE """
    
    conditions = []
    # ciclo para crear las condiciones del SQL
    for col, val in row.items():
        if col != 'text':
            if isinstance(val, str):
                conditions.append(f"{col} = '{val}'")
            elif isinstance(val, (int, float)):
                conditions.append(f"{col} = {val}")
            elif isinstance(val, datetime.date):
                conditions.append(f"{col} = '{val.isoformat()}'")
            else:
                raise TypeError(f"Tipo de dato no soportado: {type(val)} en la columna {col}")
    
    query += " AND ".join(conditions)   #Une las condiciones al query principal
    print(query)
    query_job = client.query(query)
    result = query_job.result()     #Ejecuta el SQL
    
    for row in result:
        return row[0] > 0       #si el resultado es mayor a 0 devuelve TRUE
    
def Guardar_en_BigQuery(data, dataset_id, table_id, schema):
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        tabla = bigquery_client.get_table(table_ref)
        tabla_existe = True
    except:
        tabla_existe = False

    if tabla_existe:
        print(f'----- La tabla {table_id} ya existe en el dataset {dataset_id} -----')
    else:
        print(f'----- La tabla {table_id} no existe en el dataset {dataset_id} -----')
        # Crear la tabla si no existe
        tabla = bigquery.Table(table_ref, schema=schema)
        tabla = bigquery_client.create_table(tabla)
        print(f'----- Se ha creado la tabla {table_id} en el dataset {dataset_id} -----')
        
    # Agregar los registros de data a la tabla existente o recién creada
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if tabla_existe else bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bigquery_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
    job.result()
    print(f'----- REGISTROS AGREGADOS CORRECTAMENTE EN: {table_id} -------')
    return

df_reviews1= pd.read_json('D:/PROYECTO FINAL/Google Maps/reviews-estados/review-Florida/1.json', lines=True)
df_reviews2= pd.read_json('D:/PROYECTO FINAL/Google Maps/reviews-estados/review-Florida/2.json', lines=True)

df_reviews3= pd.concat([df_reviews1[:5],df_reviews2[:5]], ignore_index=True)

df_reviews3['time'] = pd.to_datetime(df_reviews3['time'],unit='ms')     #Transforma la columna de tipo timestamp a formato datetime
df_reviews3 = df_reviews3[(df_reviews3['time'].dt.year >= 2010) & (df_reviews3['time'].dt.year <= 2021)]
    
df_reviews3= df_reviews3.drop(['pics','resp','name'], axis=1)       #Elimina la columna pics
df_reviews3= df_reviews3.rename(columns={'time': 'date', 'gmap_id':'business_id', 'rating':'stars'})
df_reviews3[['useful','funny','cool']] = 0
df_reviews3['platform']= 1

df_reviews3['user_id']= df_reviews3['user_id'].astype(str)
df_reviews3= df_reviews3[['business_id','user_id', 'date', 'stars','useful','funny','cool', 'text', 'platform']]

client = bigquery.Client()
project_id = 'proyectohenry2'
dataset_id = 'db_test'
table_id = 'reviews'
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

df_reviews3['existe_in_bigQ'] = df_reviews3.apply(lambda row: check_reg_existe_en_bigQ(row, full_table_id, client), axis=1)
df_reviews3= df_reviews3[df_reviews3['existe_in_bigQ']== False]
df_reviews3= df_reviews3.drop('existe_in_bigQ', axis=1) 

#file_name= data['name']
#bucket_name= data['bucket']
schema_review = [
        bigquery.SchemaField("business_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("stars", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("useful", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("funny", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("cool", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("text", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.INTEGER)
    ]

Guardar_en_BigQuery(df_reviews3, dataset_id, table_id, schema_review)
