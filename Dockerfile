FROM apache/airflow:2.7.0-python3.8

USER root

# Instalar dependências do Spark
RUN apt-get update && apt-get install -y curl gnupg2 && \
    apt-get install -y openjdk-11-jdk wget && \
    wget -qO - "https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz" | tar -xz -C /opt/ && \
    mv /opt/spark-* /opt/spark && \
    apt-get clean

# Instalar dependências do Spark
RUN apt-get update && apt-get install -y curl gnupg2 openjdk-11-jdk wget && \
    wget -q "https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz" && \
    tar -xzf spark-3.3.2-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.3.2-bin-hadoop3 /opt/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz && \
    apt-get clean

# Configurar variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"

# Instalar bibliotecas Python para o Airflow e PySpark
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
USER root  # Se precisar voltar a ser root para outras operações
USER airflow
WORKDIR /opt/airflow

