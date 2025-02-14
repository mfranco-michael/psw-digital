import requests
import json
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

def fetch_and_store_data(table_name, layer, bucket, api_url, partition_by, format_type, credentials_path):
    """
    Consome dados de uma API e armazena no GCS.
    """

    # Criar cliente do GCS com credenciais explícitas
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    storage_client = storage.Client(credentials=credentials)

    # Data da extração
    data_extracao = datetime.today().strftime('%Y-%m-%d')

    # Obter dados da API
    response = requests.get(api_url)
    if response.status_code != 200:
        raise RuntimeError(f"❌ ERRO: API retornou {response.status_code}")

    dados = response.json()
    file_path = f"{layer}/{table_name}/{partition_by}={data_extracao}/{table_name}.{format_type}"

    try:
        # Criar o bucket e blob no GCS
        bucket_gcs = storage_client.bucket(bucket)
        blob = bucket_gcs.blob(file_path)

        # Salvar os dados como JSON
        blob.upload_from_string(json.dumps(dados, indent=2), content_type="application/json")

        print(f"✅ Dados salvos no GCS: gs://{bucket}/{file_path}")

    except Exception as e:
        raise RuntimeError(f"❌ Falha ao salvar no GCS: {str(e)}")

