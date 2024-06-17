from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
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
    'gcs_to_bigquery_etl',
    default_args=default_args,
    description='ETL pipeline from GCS to BigQuery',
    schedule_interval=timedelta(days=1),
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

    print(f'... SE HA CARGADO EL ARCHIVO: {file_name} ...')
#########################################################################################

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

    # Se guardan las columnas categorias y el gmap_id en un df
    df_category= df_data[['gmap_id','category']]  
    df_category= df_category.explode('category')
    df_category= df_category.rename(columns={'gmap_id': 'business_id','category': 'category_name'}) # cambiar nombre de las columnas
    df_category['platform']= 1

    # Se guardan las columnas MISC y el gmap_id en un df
    df_misc= df_data[['gmap_id','MISC']]
    df_misc= df_misc.dropna(subset=['MISC']) # Elimina filas con MISC nulo

    # Funcion para cargar los json a una serie de pandas
    def expandir_diccionario(diccionario):
        return pd.Series(diccionario)

    # Se concatenas las series al df original
    df_expandido = pd.concat([df_misc, df_misc['MISC'].apply(expandir_diccionario)], axis=1)
    df_expandido.drop('MISC', axis=1, inplace=True)     # Se elimina la columna de MISC
    df_misc= df_expandido

    df_Service_options= df_misc[['gmap_id','Service options']]      # Se crea un df solo las columnas gmap_id y Service Options
    df_Service_options= df_Service_options.rename(columns={'gmap_id': 'business_id','Service options': 'service_option'}) # cambiar nombre de las columnas
    df_Service_options= df_Service_options.dropna(subset=['service_option']) # Elimina filas con columna 'Service Opciones nulas'
    df_Service_options= df_Service_options.explode('service_option')

    df_Planning= df_misc[['gmap_id','Planning']]      # Se crea un df solo las columnas gmap_id y Planning
    df_Planning= df_Planning.rename(columns={'gmap_id': 'business_id', 'Planning': 'planning_option'}) # cambiar nombre de la columna
    df_Planning= df_Planning.dropna(subset=['planning_option']) # Elimina filas con columna 'Service Opciones nulas'
    df_Planning= df_Planning.explode('planning_option')

    #Borrar Columnas
    df_data= df_data.drop(['relative_results','address', 'num_of_reviews', 'description', 'url','category', 'MISC', 'hours'], axis=1) 
    # Ordena el orden de las columnas
    df_data= df_data[['gmap_id','name', 'city', 'state', 'latitude', 'longitude', 'avg_rating', 'price']]
    df_data= df_data.rename(columns={'gmap_id': 'business_id', 'avg_rating': 'stars'}) # cambiar nombre de la columnas
    df_data['platform']= 1

    print(' ........ PROCESO DE TRANSFORMACION COMPLETADO .....')

#####################################################################################
    # Guardar el DataFrame transformado en GCS
    destination_blob_name = f'transformed_data_{datetime.now().strftime("%Y%m%d")}.csv'
    transformed_file_path = '/tmp/transformed_data.csv'
    df_data.to_csv(transformed_file_path, index=False)
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(transformed_file_path)

# Tareas del DAG
extract_transform = PythonOperator(
    task_id='extract_and_transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_to_bq = GCSToBigQueryOperator(
    task_id='load_to_bq',
    bucket='your-bucket-name',
    source_objects=['path/to/transformed_data.csv'],
    destination_project_dataset_table='your-project.your_dataset.your_table',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

extract_transform
#extract_transform >> load_to_bq