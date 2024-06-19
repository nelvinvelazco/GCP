from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
import pandas as pd
import io

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'gcs_to_bigquery_etl_with_date',
    default_args=default_args,
    description='ETL pipeline from GCS to BigQuery with dynamic filename',
    schedule_interval=None,  # No scheduling interval
    catchup=False,  # Prevent backfilling
)

# Función de transformación
def transform_data(**kwargs):
    storage_client = storage.Client()
    file_name= '1.json'
    bucket_name= 'gmaps_data2'
    # Descargar el archivo CSV desde GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string()
    
    # Crear un DataFrame de Pandas a partir del contenido del archivo json
    df_data = pd.read_json(io.BytesIO(content),lines=True)
    
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
    file_name = f'transformed_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    blob2 = bucket.blob(file_name)
    csv_buffer = io.StringIO()
    df_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    blob2.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')   

# Tareas del DAG
extract_y_transform = PythonOperator(
    task_id='extract_and_transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

"""load_to_bq = GCSToBigQueryOperator(
    task_id='load_to_bq',
    bucket='your-bucket-name',
    source_objects=['path/to/transformed_data.csv'],
    destination_project_dataset_table='your-project.your_dataset.your_table',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)"""

extract_y_transform
#extract_transform >> load_to_bq