import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io

def Transformar_data(data, df_estados):
    df_estados= df_estados.rename(columns={'nombre_largo': 'estado', 'nombre_corto':'state'}) # cambiar nombre de la columna
    df_estados= df_estados.drop(['codigos'], axis=1) # Elimina la columna
    df_estados['estado']= df_estados['estado'].convert_dtypes(convert_string=True)
    df_estados['estado']= df_estados['estado'].str.strip()  # quita los espacios vacios

    df_data= data
    df_data = df_data.loc[:, ~df_data.columns.duplicated()]
    df_data.drop_duplicates('business_id',inplace=True)

    df_data['categories'] = df_data['categories'].str.split(',')
    df_data= df_data.dropna(subset=['categories']) # Elimina datos nulos de la columna

    df_data['Es_Restaurant'] = df_data['categories'].apply(lambda x: 'Restaurants' in x)
    df_data= df_data[df_data['Es_Restaurant']]

    df_data= df_data.dropna(subset=['state']) # Elimina datos nulos de la columna
    df_data['state']= df_data['state'].str.strip()  # quita los espacios vacios

    df_data = df_data.merge(df_estados, on='state', how='left')     # Se hace un join con estados para sacar el nombre largo del estado
    df_data= df_data.drop(['state'], axis=1)    # Borra la columna
    df_data= df_data.rename(columns={'estado': 'state'}) # cambiar nombre de la columna
    df_data= df_data.dropna(subset=['state']) # Elimina datos nulos de la columna

    lista_estados= ['Florida', 'Pennsylvania', 'Tennessee', 'California', 'Texas', 'New York']
    df_data= df_data[df_data['state'].isin(lista_estados)]

    df_category= df_data[['business_id','categories']]
    df_category= df_category.explode('categories')
    df_category= df_category.dropna(subset=['categories']) # Elimina datos nulos de la columna
    df_category['categories']= df_category['categories'].str.strip()    # Elimina los espacios
    df_category= df_category.rename(columns={'categories': 'category_name'}) # cambiar nombre de las columnas

    df_atributes= df_data[['business_id','attributes']]
    def expandir_diccionario(diccionario):
        return pd.Series(diccionario)
    df_expandido = pd.concat([df_atributes, df_atributes['attributes'].apply(expandir_diccionario)], axis=1)
    df_expandido.drop('attributes', axis=1, inplace=True)     # Se elimina la columna de MISC
    df_atributes= df_expandido
    df_atributes = df_atributes[['business_id','RestaurantsDelivery','OutdoorSeating','BusinessAcceptsCreditCards','GoodForKids',
                             'RestaurantsPriceRange2','RestaurantsTakeOut','RestaurantsReservations','HasTV']]
    df_atributes= df_atributes.fillna('None')     # Se imputan los valores nulos a 'None'

    df_data= df_data.drop(['address','postal_code', 'review_count', 'is_open', 'attributes','categories', 'hours'], axis=1)
    df_data= df_data[['business_id','name', 'city', 'state', 'latitude', 'longitude', 'stars']]
    df_data['price'] = "SIN DATO"
    df_data['platform'] = 2

    print(' ........ PROCESO DE TRANSFORMACION COMPLETADO .....')
    return df_data, df_category, df_atributes


def Cargar_Data(file_name, bucket_name):
    storage_client = storage.Client()

    # Descargar el archivo CSV desde GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string()
    
    # Crear un DataFrame de Pandas a partir del contenido del archivo json
    df_data = pd.read_pickle(io.BytesIO(content))
    
    # Crear Dataframe de pandas con los estados
    blob_estados= bucket.blob('estados_usa.csv')
    df_estados= pd.read_csv(io.BytesIO(blob_estados.download_as_string()), delimiter = ';', encoding = "utf-8")
    

    print(f'... SE HA CARGADO EL ARCHIVO: {file_name} ...')

    return df_data, df_estados


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


def Procesar_Data_Business(data, context):
    file_name= data['name']
    bucket_name= data['bucket']
    dataset_id= 'BD_Henry'
    table_id = 'business'
    schema_sitios = [
        bigquery.SchemaField("business_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("city", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("state", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("latitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("longitude", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("stars", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
    ]

    schema_category = [
        bigquery.SchemaField("business_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
    ]

    schema_atributos = [
        bigquery.SchemaField("business_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("RestaurantsDelivery", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("OutdoorSeating", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("BusinessAcceptsCreditCards", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("GoodForKids", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("RestaurantsPriceRange2", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("RestaurantsTakeOut", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("RestaurantsReservations", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("HasTV", bigquery.enums.SqlTypeNames.STRING),
    ]

        
    # Carga los archivo a procesar en un dataframe 
    df_data, df_estados = Cargar_Data(file_name, bucket_name)
    # Realiza las transformaciones y limpieza del archivo y lo devuelve junto con 2 df resultantes
    df_procesado, df_category, df_atributes = Transformar_data(df_data, df_estados)
    # Guardas los datos procesados en BigQuery
    Guardar_en_BigQuery(df_procesado, dataset_id, table_id, schema_sitios)
    Guardar_en_BigQuery(df_category, dataset_id, 'category_business', schema_category)
    Guardar_en_BigQuery(df_atributes, dataset_id, 'attributes_business', schema_atributos)

