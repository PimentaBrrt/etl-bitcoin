"""
DAG: btc_etl_pipeline
=====================
Pipeline end-to-end do estudo financeiro e comportamental do Bitcoin.

Fontes:
    - API REST Binance (OHLC + volume; market cap estimado)
    - Dataset Kaggle "Bitcoin and Fear and Greed" (CSV)

Fluxo de tasks (>= 4, com dependências e paralelismo nas extrações):

    bootstrap_schema
          |
          +--> extract_coingecko ---> transform_market -----+
          |    (task lê da API Binance; nome mantido p/ compat)|
          +--> extract_kaggle_fng --> transform_sentiment ---+
                                                             |
                                                             v
                                                  validate_staging   (fail-fast DQ)
                                                             |
                                                             v
                                                       load_dim_date
                                                             |
                                                             v
                                                         load_fact
                                                             |
                                                             v
                                                      validate_mart   (integridade)

Decisões:
    - LocalExecutor: simples de subir no notebook do aluno.
    - schedule diário (@daily) com catchup desligado — atende ao requisito de
      "DAG agendada"; o disparo manual também funciona pela UI.
    - Extrações das duas fontes rodam em paralelo (são independentes).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")  # disponibiliza o pacote ``etl`` para a DAG

from etl import extract, transform, load, quality  # noqa: E402
from etl.db import bootstrap_schema  # noqa: E402

DEFAULT_ARGS = {
    "owner": "grupo_data_integration",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="btc_etl_pipeline",
    description="ESPM Data Integration 2026.1 — ETL Bitcoin (Binance + Kaggle Fear&Greed)",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",          # DAG agendada (requisito 5)
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["espm", "data-integration", "bitcoin", "etl"],
    doc_md=__doc__,
) as dag:

    t_bootstrap = PythonOperator(
        task_id="bootstrap_schema",
        python_callable=bootstrap_schema,
        doc_md="Cria schemas/tabelas (idempotente) e popula dim_sentiment.",
    )

    # --- Extração (paralela) ---
    t_extract_cg = PythonOperator(
        task_id="extract_coingecko",
        python_callable=extract.extract_coingecko,
        doc_md="Fonte 1: API REST Binance -> raw.coingecko_market (JSONB).",
    )
    t_extract_kg = PythonOperator(
        task_id="extract_kaggle_fng",
        python_callable=extract.extract_kaggle_fng,
        doc_md="Fonte 2: CSV Kaggle Fear&Greed -> raw.fear_greed (texto).",
    )

    # --- Transformação ---
    t_transform_mkt = PythonOperator(
        task_id="transform_market",
        python_callable=transform.transform_market,
        doc_md="raw.coingecko_market -> staging.btc_market (tipado).",
    )
    t_transform_snt = PythonOperator(
        task_id="transform_sentiment",
        python_callable=transform.transform_sentiment,
        doc_md="raw.fear_greed -> staging.btc_sentiment (limpo/normalizado).",
    )

    # --- Qualidade (bloqueante) ---
    t_validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=quality.validate_staging,
        doc_md="5 checagens de DQ em staging (nulos, duplicidade, ranges). Fail-fast.",
    )

    # --- Carga dimensional ---
    t_load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load.load_dim_date,
        doc_md="Popula mart.dim_date.",
    )
    t_load_fact = PythonOperator(
        task_id="load_fact",
        python_callable=load.load_fact,
        doc_md="Popula mart.fact_btc_daily (join + métricas derivadas + UPSERT).",
    )

    # --- Qualidade pós-carga ---
    t_validate_mart = PythonOperator(
        task_id="validate_mart",
        python_callable=quality.validate_mart,
        doc_md="Integridade referencial do star schema.",
    )

    # ---- Dependências ----
    t_bootstrap >> [t_extract_cg, t_extract_kg]
    t_extract_cg >> t_transform_mkt
    t_extract_kg >> t_transform_snt
    [t_transform_mkt, t_transform_snt] >> t_validate_staging
    t_validate_staging >> t_load_dim_date >> t_load_fact >> t_validate_mart
