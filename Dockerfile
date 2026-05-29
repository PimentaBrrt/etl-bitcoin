# =====================================================================
# Imagem do Airflow + dependências do ETL do Bitcoin
# Base oficial do Apache Airflow 2.9 com Python 3.11
# =====================================================================
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl build-essential libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# As versoes em requirements.txt ja estao alinhadas as constraints oficiais do
# Airflow 2.9.3 (SQLAlchemy 1.4.52, pandas 2.1.4, numpy 1.26.4), entao o install
# direto basta e nao depende de baixar o arquivo de constraints remoto.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
