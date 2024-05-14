from google.cloud import bigquery
import os
import re
import datetime
import json 


def cargar_data(client,dataset_name_only_20, file_name):
    print("dataset_name_only_20" + dataset_name_only_20)
    dataset = dataset_name_only_20 
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("count", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("nif", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("cp", "STRING"),
        bigquery.SchemaField("color", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("ssn", "STRING"),
        bigquery.SchemaField("count_bank", "STRING"),
    ]