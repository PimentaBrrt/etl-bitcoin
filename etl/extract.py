"""
extract.py — Extração das DUAS fontes heterogêneas (requisito 1 do briefing).

Fonte 1 — API REST Binance (estruturada, dinâmica):
    Endpoint /api/v3/klines (candles diários), público e sem chave:
      - GET /api/v3/klines?symbol=BTCUSDT&interval=1d&startTime=..&endTime=..
            -> retorna [[open_time, open, high, low, close, volume, ...], ...]
    Escolhemos a Binance no lugar da CoinGecko porque o plano gratuito da
    CoinGecko só fornece dados diários dos últimos 365 dias, o que NÃO tem
    interseção temporal com o dataset de sentimento (Fear & Greed, 2018–2023).
    A Binance fornece OHLCV diário desde 2017, permitindo o join por data.
    Observação: klines não traz market cap; nós o aproximamos como
    close * circulating_supply (constante parametrizável), o que é suficiente
    para a análise comportamental proposta.

Fonte 2 — Dataset Kaggle "Bitcoin and Fear and Greed" (semi-estruturada via CSV):
    https://www.kaggle.com/datasets/adilbhatti/bitcoin-and-fear-and-greed
    Lemos o CSV cru. Usamos as colunas Value e Value_Classification (índice de
    medo e ganância) — atende ao requisito 2 (fonte semi/não estruturada, pois o
    CSV chega "sujo": com nulos, formatos textuais, sem tipagem).

Ambas as fontes são gravadas CRUAS na camada `raw` (JSONB e texto), preservando o
princípio de não perder o dado original.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from etl.config import MK, PATHS
from etl.db import get_psycopg2_conn


# =============================================================================
# FONTE 1 — Binance (API REST pública, klines diários)
# =============================================================================

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
def _get_klines(start_ms: int, end_ms: int) -> list:
    """Uma página de candles diários da Binance (até 1000 por chamada)."""
    url = MK.base_url.rstrip("/") + "/api/v3/klines"
    params = {
        "symbol": MK.symbol,
        "interval": "1d",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _fetch_market() -> pd.DataFrame:
    """
    Busca OHLCV diário da Binance no intervalo [MK.start_date, MK.end_date],
    paginando de 1000 em 1000 dias. Retorna DataFrame com as colunas que o
    transform espera: market_date, open/high/low/close/volume/market_cap (USD).
    """
    start = datetime.fromisoformat(MK.start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(MK.end_date).replace(tzinfo=timezone.utc)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        page = _get_klines(cursor, end_ms)
        if not page:
            break
        rows.extend(page)
        last_open = page[-1][0]
        cursor = last_open + 86_400_000  # avança 1 dia após o último candle
        if len(page) < 1000:
            break

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open_usd", "high_usd", "low_usd", "close_usd",
            "volume_btc", "close_time", "quote_volume", "trades",
            "taker_base", "taker_quote", "ignore",
        ],
    )
    df["market_date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.date
    for c in ("open_usd", "high_usd", "low_usd", "close_usd", "quote_volume"):
        df[c] = df[c].astype(float)
    # volume em USD: a Binance traz "quote_volume" (volume já em USDT ~ USD)
    df["volume_usd"] = df["quote_volume"]
    # market cap aproximado: preço de fechamento * supply circulante (constante)
    df["market_cap_usd"] = df["close_usd"] * MK.circulating_supply
    df = df.drop_duplicates("market_date").sort_values("market_date")
    return df[
        ["market_date", "open_usd", "high_usd", "low_usd", "close_usd",
         "volume_usd", "market_cap_usd"]
    ]


def extract_coingecko(**_) -> dict:
    """
    Task de extração da fonte de mercado (Binance).
    Mantém o nome `extract_coingecko` para não alterar a DAG; grava 1 linha
    JSONB por dia em raw.coingecko_market (mesmo formato de payload de antes).
    """
    logger.info(
        f"[extract:market] fonte=binance symbol={MK.symbol} "
        f"janela={MK.start_date}..{MK.end_date}"
    )
    merged = _fetch_market()
    if merged.empty:
        logger.warning("[extract:market] nenhum candle retornado pela Binance")
        return {"source": "binance", "rows": 0}
    logger.info(f"[extract:market] {len(merged)} dias consolidados da API")

    conn = get_psycopg2_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            # idempotência: limpa raw antes de reinserir a janela de dias
            cur.execute("DELETE FROM raw.coingecko_market")
            for _, row in merged.iterrows():
                payload = {
                    "market_date": str(row["market_date"]),
                    "open": float(row["open_usd"]),
                    "high": float(row["high_usd"]),
                    "low": float(row["low_usd"]),
                    "close": float(row["close_usd"]),
                    "volume": float(row["volume_usd"]),
                    "market_cap": float(row["market_cap_usd"]),
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                cur.execute(
                    "INSERT INTO raw.coingecko_market(market_date, payload) VALUES (%s, %s)",
                    [row["market_date"], json.dumps(payload)],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(f"[extract:market] {inserted} dias gravados em raw.coingecko_market")
    return {"source": "binance", "rows": inserted}


# =============================================================================
# FONTE 2 — Kaggle Fear & Greed (CSV)
# =============================================================================

def extract_kaggle_fng(csv_path: str | None = None, **_) -> dict:
    """
    Task de extração do dataset Kaggle (CSV).
    Lê o CSV cru e grava em raw.fear_greed como TEXTO (sem tipar nada ainda).
    Mantemos tudo como string para que a camada de transform faça a limpeza,
    evidenciando os problemas de qualidade (nulos, tipos) do dado bruto.
    """
    csv_path = csv_path or PATHS.seed_csv
    logger.info(f"[extract:kaggle] lendo CSV {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)  # tudo como string de propósito
    logger.info(f"[extract:kaggle] {len(df)} linhas no CSV bruto")

    conn = get_psycopg2_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM raw.fear_greed")
            for _, r in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO raw.fear_greed
                        (market_date, value, value_classification, btc_closing, btc_volume)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        r.get("Date"),
                        r.get("Value"),
                        r.get("Value_Classification"),
                        r.get("BTC_Closing"),
                        r.get("BTC_Volume"),
                    ],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(f"[extract:kaggle] {inserted} linhas gravadas em raw.fear_greed")
    return {"source": "kaggle_fng", "rows": inserted}


if __name__ == "__main__":
    print(extract_kaggle_fng())
    print(extract_coingecko())
