"""
demo_run.py — Executa o pipeline inteiro fora do Airflow, em sequência.

Útil para:
  - depurar rapidamente sem subir o scheduler;
  - a demonstração ao vivo da apresentação;
  - rodar as 3 consultas de valor e imprimir os resultados no terminal.

Uso (dentro do container, com o Postgres no ar):
    python /opt/airflow/demo_run.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append("/opt/airflow")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from loguru import logger

from etl.db import bootstrap_schema, get_engine
from etl import extract, transform, load, quality


def run_pipeline() -> None:
    logger.info("=== 1/8 bootstrap do schema ===")
    bootstrap_schema()

    logger.info("=== 2/8 extract Binance (API) ===")
    extract.extract_coingecko()

    logger.info("=== 3/8 extract Kaggle Fear&Greed (CSV) ===")
    extract.extract_kaggle_fng()

    logger.info("=== 4/8 transform market ===")
    transform.transform_market()

    logger.info("=== 5/8 transform sentiment ===")
    transform.transform_sentiment()

    logger.info("=== 6/8 validate staging (DQ) ===")
    quality.validate_staging()

    logger.info("=== 7/8 load dimensional ===")
    load.load_dim_date()
    load.load_fact()

    logger.info("=== 8/8 validate mart (integridade) ===")
    quality.validate_mart()

    logger.success("Pipeline concluído. Rodando as 3 consultas de valor...\n")
    run_value_queries()


def run_value_queries() -> None:
    import pandas as pd

    eng = get_engine()
    queries = {
        "CONSULTA 1 — Retorno medio por sentimento": """
            SELECT ds.classification AS sentimento, COUNT(*) AS dias,
                   ROUND(AVG(f.daily_return_pct),4) AS retorno_medio_pct,
                   ROUND(STDDEV_POP(f.daily_return_pct),4) AS volatilidade_pct
            FROM mart.fact_btc_daily f
            JOIN mart.dim_sentiment ds ON f.sentiment_key = ds.sentiment_key
            GROUP BY ds.classification, ds.sentiment_order
            ORDER BY ds.sentiment_order
        """,
        "CONSULTA 2 — Volume medio por faixa do indice": """
            SELECT CASE
                     WHEN f.fng_value < 25 THEN '0-24 Medo Extremo'
                     WHEN f.fng_value < 45 THEN '25-44 Medo'
                     WHEN f.fng_value < 55 THEN '45-54 Neutro'
                     WHEN f.fng_value < 75 THEN '55-74 Ganancia'
                     ELSE '75-100 Ganancia Extrema' END AS faixa,
                   COUNT(*) AS dias,
                   ROUND(AVG(f.volume_usd)/1e9,2) AS volume_medio_bi_usd,
                   ROUND(AVG(f.close_usd),2) AS preco_medio_usd
            FROM mart.fact_btc_daily f
            WHERE f.fng_value IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """,
        "CONSULTA 3 — Resumo anual": """
            SELECT dd.year AS ano, COUNT(*) AS dias,
                   ROUND(AVG(f.close_usd),2) AS preco_medio_usd,
                   ROUND(AVG(f.fng_value),1) AS fng_medio
            FROM mart.fact_btc_daily f
            JOIN mart.dim_date dd ON f.date_key = dd.date_key
            GROUP BY dd.year ORDER BY dd.year
        """,
    }
    for title, sql in queries.items():
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
        print(pd.read_sql(sql, eng).to_string(index=False))
    print()


if __name__ == "__main__":
    run_pipeline()
