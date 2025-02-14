import yaml
import os
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# 🔹 Função para carregar configurações YAML
def load_yaml(yaml_path):
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    # Criar credenciais diretamente a partir do YAML
    credentials = service_account.Credentials.from_service_account_file(config["CREDENTIALS_PATH"])

    return config, credentials


# 🔹 Função para encontrar a última partição no GCS
def get_latest_partition(storage_client, bucket_name, table_name):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"bronze/{table_name}/")

    partitions = sorted(set(blob.name.split('/')[2] for blob in blobs if "dt_extract=" in blob.name))

    if not partitions:
        raise FileNotFoundError("❌ Nenhuma partição válida encontrada no GCS!")

    latest_partition = partitions[-1]
    print(f"📌 Última partição encontrada: {latest_partition}")
    return latest_partition


# 🔹 Função para carregar JSON do GCS diretamente no PySpark
def load_json_from_gcs(storage_client, spark, bucket_name, table_name, latest_partition):
    gcs_path = f"bronze/{table_name}/{latest_partition}/{table_name}.json"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    json_data = blob.download_as_text()

    # Criar um RDD e converter para DataFrame PySpark
    rdd = spark.sparkContext.parallelize([json_data])
    df_spark = spark.read.json(rdd)

    # Exibir o schema e amostra de dados
    df_spark.printSchema()
    return df_spark


# 🔹 Função para aplicar transformações no DataFrame PySpark
def apply_transforms(df_spark, transformations):
    for column in transformations['columns']:
        column_name = column['name']
        new_name = column.get('rename_to', column_name)
        data_type = column['type']

        if data_type == "string":
            df_spark = df_spark.withColumn(new_name, col(column_name).cast("string"))
        elif data_type == "float":
            df_spark = df_spark.withColumn(new_name, col(column_name).cast("float"))
        elif data_type == "date":
            df_spark = df_spark.withColumn(new_name, col(column_name).cast("date"))

        df_spark = df_spark.drop(column_name)

    return df_spark


# 🔹 Função para salvar no BigQuery
def save_to_bigquery(df_pandas, bq_client, config, table_name):
    tabela_completa = f"{config['BQ_PROJECT']}.{config['BQ_DATASET']}.{table_name}"
    
    print(f"🚀 Enviando dados para {tabela_completa}... (sem particionamento por nm_state)")
    
    try:
        to_gbq(
            df_pandas, 
            tabela_completa, 
            project_id=config['BQ_PROJECT'], 
            if_exists="replace",  # Mudar para 'append' se necessário
            credentials=bq_client._credentials, 
            progress_bar=True
        )
        print(f"✅ DataFrame salvo com sucesso no BigQuery: {tabela_completa}")
    except Exception as e:
        print(f"❌ Erro ao salvar no BigQuery: {e}")


# 🔹 Função principal para transformação Bronze → Silver
def transform_to_silver(yaml_path):
    # Carregar configuração e credenciais
    config, credentials = load_yaml(yaml_path)

    # Criar clientes para GCS e BigQuery
    storage_client = storage.Client(credentials=credentials)
    bq_client = bigquery.Client(credentials=credentials, project=config["BQ_PROJECT"])

    # Criar sessão Spark
    spark = SparkSession.builder.appName("Transform Bronze to Silver").getOrCreate()

    table_name = config["table_name"]

    # Encontra a última partição no GCS
    latest_partition = get_latest_partition(storage_client, config["GCS_BUCKET"], table_name)

    # Carregar JSON do GCS para PySpark
    df_spark = load_json_from_gcs(storage_client, spark, config["GCS_BUCKET"], table_name, latest_partition)

    # Aplicar transformações
    df_silver_spark = apply_transforms(df_spark, config)

    # Converter para Pandas e salvar no BigQuery
    df_silver_pandas = df_silver_spark.toPandas()
    save_to_bigquery(df_silver_pandas, bq_client, config, table_name)

    print('✅ Processo concluído.')



