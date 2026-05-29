"""
load.py — Carga na modelagem dimensional (star schema).

Popula, nesta ordem:
  1. mart.dim_date       (a partir das datas presentes no mercado)
  2. mart.fact_btc_daily (join de staging.btc_market + staging.btc_sentiment,
                          resolvendo as FKs para dim_date e dim_sentiment,
                          e calculando as métricas derivadas)

dim_sentiment já vem populada pelo seed (bootstrap).

Carga é idempotente: usa UPSERT por chave natural (date_key). Isso também
suporta o "nice-to-have" de carga incremental — reexecuções só atualizam/
inserem os dias da janela, sem duplicar.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from etl.db import get_engine, get_psycopg2_conn
from etl.transform import daily_return_pct, intraday_range_pct

_MONTHS = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
           "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
_WEEKDAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]


def load_dim_date(**_) -> dict:
    """Popula dim_date a partir das datas do mercado (staging.btc_market)."""
    eng = get_engine()
    dates = pd.read_sql("SELECT DISTINCT market_date FROM staging.btc_market", eng)
    if dates.empty:
        logger.warning("[load:dim_date] sem datas em staging")
        return {"rows": 0}

    conn = get_psycopg2_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            for d in pd.to_datetime(dates["market_date"]):
                date_key = int(d.strftime("%Y%m%d"))
                cur.execute(
                    """
                    INSERT INTO mart.dim_date
                        (date_key, full_date, year, quarter, month, month_name,
                         day, day_of_week, weekday_name, is_weekend)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (date_key) DO NOTHING
                    """,
                    [
                        date_key, d.date(), d.year, (d.month - 1) // 3 + 1, d.month,
                        _MONTHS[d.month], d.day, d.weekday(), _WEEKDAYS[d.weekday()],
                        d.weekday() >= 5,
                    ],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(f"[load:dim_date] {inserted} datas processadas")
    return {"rows": inserted}


def load_fact(**_) -> dict:
    """
    Popula fact_btc_daily fazendo LEFT JOIN do mercado com o sentimento.
    (LEFT para que dias com mercado mas sem sentimento ainda entrem no fato.)
    """
    eng = get_engine()
    df = pd.read_sql(
        """
        SELECT m.market_date,
               m.open_usd, m.high_usd, m.low_usd, m.close_usd,
               m.volume_usd, m.market_cap_usd,
               s.fng_value, s.fng_classification
        FROM staging.btc_market m
        LEFT JOIN staging.btc_sentiment s USING (market_date)
        ORDER BY m.market_date
        """,
        eng,
    )
    if df.empty:
        logger.warning("[load:fact] sem dados de mercado")
        return {"rows": 0, "matched_sentiment": 0}

    # resolve sentiment_key
    sent_map = dict(
        pd.read_sql(
            "SELECT classification, sentiment_key FROM mart.dim_sentiment", eng
        ).values
    )

    conn = get_psycopg2_conn()
    inserted = 0
    matched = 0
    try:
        with conn, conn.cursor() as cur:
            for _, r in df.iterrows():
                d = pd.to_datetime(r["market_date"])
                date_key = int(d.strftime("%Y%m%d"))
                sentiment_key = sent_map.get(r["fng_classification"]) if pd.notna(
                    r["fng_classification"]
                ) else None
                if sentiment_key is not None:
                    matched += 1
                ret = daily_return_pct(float(r["open_usd"]), float(r["close_usd"]))
                rng = intraday_range_pct(float(r["high_usd"]), float(r["low_usd"]))
                fng = int(r["fng_value"]) if pd.notna(r["fng_value"]) else None
                cur.execute(
                    """
                    INSERT INTO mart.fact_btc_daily
                        (date_key, sentiment_key, open_usd, high_usd, low_usd,
                         close_usd, volume_usd, market_cap_usd,
                         daily_return_pct, intraday_range_pct, fng_value)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (date_key) DO UPDATE SET
                        sentiment_key      = EXCLUDED.sentiment_key,
                        open_usd           = EXCLUDED.open_usd,
                        high_usd           = EXCLUDED.high_usd,
                        low_usd            = EXCLUDED.low_usd,
                        close_usd          = EXCLUDED.close_usd,
                        volume_usd         = EXCLUDED.volume_usd,
                        market_cap_usd     = EXCLUDED.market_cap_usd,
                        daily_return_pct   = EXCLUDED.daily_return_pct,
                        intraday_range_pct = EXCLUDED.intraday_range_pct,
                        fng_value          = EXCLUDED.fng_value,
                        loaded_at          = now()
                    """,
                    [
                        date_key, sentiment_key, r["open_usd"], r["high_usd"],
                        r["low_usd"], r["close_usd"], r["volume_usd"],
                        r["market_cap_usd"], ret, rng, fng,
                    ],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(
        f"[load:fact] {inserted} fatos carregados "
        f"({matched} com sentimento associado)"
    )
    return {"rows": inserted, "matched_sentiment": matched}


if __name__ == "__main__":
    print(load_dim_date())
    print(load_fact())
