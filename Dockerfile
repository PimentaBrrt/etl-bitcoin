# =====================================================================
# Imagem do Airflow + dependencias do ETL do Bitcoin
# Base oficial do Apache Airflow 2.9 com Python 3.11
# =====================================================================
FROM apache/airflow:2.9.3-python3.11

# --- Pacotes de sistema (como root) ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl build-essential libpq-dev \
        postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Script que espera o Postgres e DELEGA ao entrypoint oficial (/entrypoint).
# Mantemos o entrypoint oficial no fluxo para que as variaveis
# _AIRFLOW_DB_MIGRATE e _AIRFLOW_WWW_USER_* sejam processadas (criam o admin).
COPY docker/entrypoint.sh /wait-and-entrypoint.sh
RUN chmod +x /wait-and-entrypoint.sh

# --- Dependencias Python (como airflow) ---
USER airflow

# Versoes alinhadas as constraints oficiais do Airflow 2.9.3
# (SQLAlchemy 1.4.52, pandas 2.1.4, numpy 1.26.4).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/wait-and-entrypoint.sh"]
