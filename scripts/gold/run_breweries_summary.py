from google.cloud import bigquery
from google.oauth2 import service_account

# 🔹 Configurações
CREDENTIALS_PATH = "/opt/spark/scripts/gold/pswdigital-b6c97c6db155.json"
PROJECT_ID = "pswdigital"
DATASET = "gold"

# 🔹 Criar credenciais e cliente do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# 🔹 Consulta SQL embutida no código
sql_query = """
CREATE OR REPLACE TABLE `pswdigital.gold.breweries_summary`
CLUSTER BY nm_state, nm_city, nm_country, tp_brewery
AS
SELECT nm_country, -- País da cervejaria
       nm_state,   -- Estado da cervejaria
       nm_city,    -- Cidade da cervejaria
       tp_brewery, -- Tipo de cervejaria
       COUNT(*) OVER (PARTITION BY nm_city, nm_state, nm_country, tp_brewery) qt_total_city_and_type,    -- Total por cidade e tipo de cervejaria
       COUNT(*) OVER (PARTITION BY nm_state, nm_country, tp_brewery)          qt_total_state_and_type,   -- Total por estado e tipo de cervejaria
       COUNT(*) OVER (PARTITION BY nm_country, tp_brewery)                    qt_total_country_and_type, -- Total por país e tipo de cervejaria
       COUNT(*) OVER (PARTITION BY nm_city, nm_state, nm_country)             qt_total_city,             -- Total por cidade
       COUNT(*) OVER (PARTITION BY nm_state, nm_country)                      qt_total_state,            -- Total por estado
       COUNT(*) OVER (PARTITION BY nm_country)                                qt_total_country,          -- Total por país
       COUNT(*) OVER (PARTITION BY tp_brewery)                                qt_total_type              -- Total por tipo de cervejaria
FROM `pswdigital.silver.breweries`;
"""

# 🔹 Executar a consulta no BigQuery
try:
    query_job = client.query(sql_query)  # Enviar a consulta SQL
    query_job.result()  # Esperar a execução
    print(f"✅ Tabela 'breweries_summary' criada/atualizada com sucesso no BigQuery!")
except Exception as e:
    print(f"❌ Erro ao executar o SQL no BigQuery: {e}")

