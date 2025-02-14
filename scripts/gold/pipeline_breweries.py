from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Nome da tabela
TABLE_NAME = "breweries"

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 10),
    'retries': 1,
}

dag = DAG(
    dag_id=f"pipeline_{TABLE_NAME}",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

# Task para rodar o script Bronze (Spark)
load_bronze_task = BashOperator(
    task_id=f"load_bronze_{TABLE_NAME}",
    bash_command="docker exec spark /opt/bitnami/spark/bin/spark-submit /opt/spark/scripts/bronze/breweries.py",
    dag=dag,
)

# Task para rodar o script Silver (PySpark + BigQuery)
load_silver_task = BashOperator(
    task_id=f"load_silver_{TABLE_NAME}",
    bash_command="docker exec spark /opt/bitnami/spark/bin/spark-submit /opt/spark/scripts/silver/breweries.py /opt/spark/scripts/silver/breweries.yaml",
    dag=dag,
)

# Task para rodar o script Gold (SQL BigQuery via Spark)
run_breweries_summary_task = BashOperator(
    task_id=f"run_breweries_summary_{TABLE_NAME}",
    bash_command="docker exec spark /opt/bitnami/spark/bin/spark-submit /opt/spark/scripts/gold/run_breweries_summary.py",
    dag=dag,
)

# Define a dependência: primeiro Bronze, depois Silver, e por fim Gold
load_bronze_task >> load_silver_task >> run_breweries_summary_task

