import yaml
import os
import sys
from ingestion_bronze import fetch_and_store_data

# Caminho para o YAML da tabela
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "breweries.yaml")

# Ler configuração do YAML
def load_config():
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)

# Executar o processo
if __name__ == "__main__":
    config = load_config()
    
    fetch_and_store_data(
        config["table_name"],
        config["layer"],
        config["bucket"],
        config["api_url"],
        config["partition_by"],
        config["format_type"],
        config["credentials_path"]
    )

