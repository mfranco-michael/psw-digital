
# Projeto de Processamento de Dados de Cervejarias

Este projeto foi desenvolvido com o objetivo de processar dados de cervejarias e armazená-los em diferentes camadas utilizando o Apache Airflow, Docker, PySpark e BigQuery. 

## Mudança de Arquitetura

Originalmente, o processamento de dados foi realizado utilizando **Trino** para consultas e **Minio** para simulação de armazenamento. Após avaliação, a arquitetura foi revista e migrou-se para o **BigQuery**, que oferece maior flexibilidade e desempenho para grandes volumes de dados. A decisão de migração foi tomada devido à maior escalabilidade e melhores opções de análise em BigQuery.

O processo foi automatizado com **Apache Airflow** em conjunto com **Docker**, facilitando a execução de workflows e orquestração de tarefas. Utilizou-se **PySpark** para o processamento de dados em larga escala.

## Arquitetura do Sistema

A arquitetura é composta por três camadas de processamento:

1. **Camada Bronze**: Ingestão e armazenamento inicial de dados em formato JSON.
2. **Camada Silver**: Transformação dos dados, aplicação de regras de negócios e carregamento para o BigQuery.
3. **Camada Gold**: Geração de visão agregada e análises, com indicadores por tipo de cervejaria, cidade, estado e país.

## Nomenclatura das Colunas

Para a padronização e organização dos dados, utilizou-se a seguinte nomenclatura para as colunas:

- `tp_` para tipo de dado (exemplo: tipo de cervejaria).
- `nm_` para string (exemplo: nome da cidade, nome da cervejaria).
- `id_` para chave única (exemplo: id da cervejaria).
- `dt_` para data (exemplo: data de criação).
- `ft_` para valores flutuantes (exemplo: latitude e longitude).
- `vl_` para valores (não aplicável, mas reservado para futuros casos).
- `nr_` para números (exemplo: número de cervejarias).
- `qt_` para quantidade (exemplo: quantidade de cervejarias).
  
Essas convenções garantem consistência e clareza nas tabelas e análises.

## Detalhes do Pipeline

O pipeline de dados é dividido nas seguintes etapas:

1. **Ingestão e Processamento (Bronze)**: Os dados são ingeridos em formato JSON e carregados no GCS (Google Cloud Storage). O processo de ingestão é realizado via Spark.
2. **Transformação e Análise (Silver)**: Após a ingestão, as transformações são realizadas em Spark e os dados são carregados no BigQuery.
3. **Criação de Visões Agregadas (Gold)**: A camada Gold agrega os dados com métricas como o total de cervejarias por tipo, cidade, estado e país, usando SQL no BigQuery.

## Detalhes sobre as Tabelas no BigQuery

- **Tabela Silver**: Armazena os dados transformados e é onde ocorre a análise básica. 
  - `nm_state`: Estado.
  - `nm_city`: Cidade.
  - `nm_country`: País.
  - `tp_brewery`: Tipo de cervejaria.
  - `nm_name`: Nome da cervejaria.
  - `nm_address_1`, `nm_address_2`, `nm_address_3`: Endereço da cervejaria.

- **Tabela Gold**: Contém as agregações de dados, com métricas como total de cervejarias por tipo e localização.

```sql
CREATE OR REPLACE TABLE `pswdigital.gold.breweries_summary`

CLUSTER BY nm_state
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
FROM `pswdigital.silver.breweries`
```

## Tecnologias Utilizadas

- **Docker**: Contêinerização do ambiente de execução.
- **Apache Airflow**: Orquestração e automação do pipeline de dados.
- **PySpark**: Processamento distribuído de dados em larga escala.
- **BigQuery**: Armazenamento e análise de dados em larga escala.
- **GitHub**: Repositório para versionamento e colaboração.

## Configuração do Ambiente

### Dependências

Antes de rodar o projeto, instale as dependências utilizando o arquivo `requirements.txt`.

```bash
pip install -r requirements.txt
```

### Estrutura do Projeto

A estrutura do projeto é organizada da seguinte forma:

```
psw-digital/
│
├── config/                 # Configurações gerais (YAML de tabelas, etc.)
├── dags/                   # Arquivos de DAGs do Airflow
│   └── pipeline_breweries.py
├── data/                   # Diretório de dados (não versionado por segurança)
├── docker-compose.yml      # Arquivo para orquestração do Docker
├── Dockerfile              # Arquivo para construção da imagem Docker
├── logs/                   # Logs de execução do Airflow
├── scripts/                # Scripts de processamento (bronze, silver, gold)
│   ├── bronze/
│   ├── gold/
│   └── silver/
├── requirements.txt        # Dependências do projeto
└── README.md               # Documentação do projeto
```

### Como Executar

1. **Subir os containers**: Utilize o `docker-compose` para subir o Airflow, Spark e PostgreSQL.

```bash
docker-compose up -d
```

2. **Executar as DAGs**: Acesse o Airflow Web UI e execute as DAGs conforme necessário.

## Observações

- **Segurança**: O arquivo `pswdigital-b6c97c6db155.json` não foi incluído no repositório por questões de segurança. O arquivo deve ser fornecido localmente ou via variáveis de ambiente.
- **Limitações**: O BigQuery exige particionamento em colunas do tipo `DATE`, `DATETIME` ou `TIMESTAMP`. Portanto, não é possível particionar diretamente por `nm_state` ou outras colunas de tipo `STRING`.

# psw-digital
