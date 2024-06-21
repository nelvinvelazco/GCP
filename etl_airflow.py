from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
import pandas as pd
import io

# Configuración del DAG
default_args = {
    'owner': 'Nelvin Velazco',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


dag = DAG(
    'Test_ETL_GCP',
    default_args=default_args,
    description='ETL para cargar un archivo de una bucket, procesarlo y guardarlo en BigQuery',
    schedule_interval=None,  # No scheduling interval
    catchup=False,  # Prevent backfilling
)

BUCKET_NAME= 'gmaps_data2'
DATASET_NAME= 'db_test'
storage_client = storage.Client()
schema_busines = [
        bigquery.SchemaField("business_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("city", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("state", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("latitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("longitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("stars", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.INTEGER),
    ]

# Función de transformación
def transformar_data(**kwargs):
    file_name= '1.json'
    # Descargar el archivo CSV desde GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    content = blob.download_as_string()
    
    # Crear un DataFrame de Pandas a partir del contenido del archivo json
    df_data = pd.read_json(io.StringIO(content.decode('utf-8')),lines=True)
    
    # Crear Dataframe de pandas con los estados
    blob_estados= bucket.blob('estados_usa.csv')
    df_estados= pd.read_csv(io.BytesIO(blob_estados.download_as_string()), delimiter = ';', encoding = "utf-8") 

    df_data.drop_duplicates('gmap_id',inplace=True)
    df_data= df_data.dropna(subset=['address']) # Elimina filas con address nulas
    df_data= df_data.dropna(subset=['category']) # Elimina filas con category nulas

    # Crea una columna re ddice si la categoria es "Restaurant" o no
    df_data['Es_Restaurant'] = df_data['category'].apply(lambda x: 'Restaurant' in x)
    df_data= df_data[df_data['Es_Restaurant']]
    df_data= df_data.fillna({'price':'SIN DATO', 'state':'SIN DATO'})     # Se imputan los valores nulos a 'SIN DATO'

    # En pruebas se detecto que esta fila estaba dando problemas con la funcion para extraer, asi que se procede a eliminarla
    df_data= df_data[df_data['address'] != "〒10028 New York, Lexington Ave, (New) Ichie Japanese Restaurant"]

    # Funcion para sacar cuidad y estado de la direccion
    def Ext_Ciudad_Estado(dir):
        ciudad= "SIN DATO"
        estado= "SIN DATO"     
        if len(str(dir)) > 10:  
            lista= str(dir).split(',')
            if len(lista) > 2:
                codigo= lista[-1][1:3]
                df_filtro= df_estados[df_estados['nombre_corto'].str.contains(codigo)]        
                if not df_filtro.empty:            
                    ciudad = lista[-2].strip()
                    estado= df_filtro.nombre_largo.values[0].strip()
        return ciudad, estado
        
    # Extraer ciudad y estado de la columna direccion y guardarda en 2 columnas
    df_data[['city','state']] = df_data.apply(lambda x: Ext_Ciudad_Estado(x['address']), axis=1, result_type='expand')
    lista_estados= ['Florida', 'Pennsylvania', 'Tennessee', 'California', 'Texas', 'New York']
    df_data= df_data[df_data['state'].isin(lista_estados)]      #Selecionar solo los estados definidos en la lista
    #Borrar Columnas
    df_data= df_data.drop(['relative_results','address', 'num_of_reviews', 'description', 'url','category', 'MISC', 'hours'], axis=1) 
    # Ordena el orden de las columnas
    df_data= df_data[['gmap_id','name', 'city', 'state', 'latitude', 'longitude', 'avg_rating', 'price']]
    df_data= df_data.rename(columns={'gmap_id': 'business_id', 'avg_rating': 'stars'}) # cambiar nombre de la columnas
    df_data['platform']= 1
    print(' ........ PROCESO DE TRANSFORMACION COMPLETADO .....')

    #####################################################################
    data_procesada = f'temp/transformed_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    blob2 = bucket.blob(data_procesada)
    csv_buffer = io.StringIO()
    df_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    blob2.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    kwargs['ti'].xcom_push(key='archivo_procesado', value= data_procesada)   

def guardar_BigQuery(**kwargs):
    file_name = kwargs['ti'].xcom_pull(key='archivo_procesado', task_ids='extraer_y_transformar')
    bucket2 = storage_client.bucket(BUCKET_NAME)
    blob_csv= bucket2.blob(file_name)
    data= pd.read_csv(io.BytesIO(blob_csv.download_as_string()), delimiter = ',', encoding = "utf-8")

    TABLE_ID = 'business'
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(DATASET_NAME).table(TABLE_ID)
    try:
        tabla = bigquery_client.get_table(table_ref)
        tabla_existe = True
    except:
        tabla_existe = False

    if tabla_existe:
        print(f'----- La tabla {TABLE_ID} ya existe en el dataset {DATASET_NAME} -----')
    else:
        print(f'----- La tabla {TABLE_ID} no existe en el dataset {DATASET_NAME} -----')
        # Crear la tabla si no existe
        tabla = bigquery.Table(table_ref, schema=schema_busines)
        tabla = bigquery_client.create_table(tabla)
        print(f'----- Se ha creado la tabla {TABLE_ID} en el dataset {DATASET_NAME} -----')
        
    # Agregar los registros de data a la tabla existente o recién creada
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if tabla_existe else bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bigquery_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
    job.result()
    print(f'----- REGISTROS AGREGADOS CORRECTAMENTE EN: {TABLE_ID} -------') 

# Tareas del DAG
extraer_y_transformar = PythonOperator(
    task_id='extraer_y_transformar',
    python_callable=transformar_data,
    provide_context=True,
    dag=dag,
)

cargar_en_BigQuery = PythonOperator(
    task_id='Cargar_en_BigQuery',
    python_callable=guardar_BigQuery,
    provide_context=True,
    dag=dag,
)

extraer_y_transformar >> cargar_en_BigQuery