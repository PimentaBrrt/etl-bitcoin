"""
transform.py — Transformação / limpeza / regras de negócio.

Lê a camada `raw`, aplica tipagem, limpeza e regras de negócio, e popula a
camada `staging` (tipada, 1 linha por dia).

Regras de negócio aplicadas:
  R1. Tipagem: textos do CSV viram INTEGER/NUMERIC; timestamps viram DATE.
  R2. Normalização do sentimento: trim + capitalização canônica das 5 classes.
  R3. Descarte de linhas sem data ou sem valor de sentimento (não tipável).
  R4. Métricas derivadas (calculadas na carga do fato):
        - daily_return_pct   = (close - open) / open * 100
        - intraday_range_pct = (high - low) / low * 100
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from etl.db import get_engine, get_psycopg2_conn

# Mapa de normalização das classes do Fear & Greed Index
_CANON_SENTIMENT = {
    "extreme fear": "Extreme Fear",
    "fear": "Fear",
    "neutral": "Neutral",
    "greed": "Greed",
    "extreme greed": "Extreme Greed",
}


def _to_int(x) -> int | None:
    try:
        if x is None or str(x).strip().lower() in ("", "nan", "none"):
            return None
        return int(round(float(x)))
    except (ValueError, TypeError):
        return None


def _canon_sentiment(x) -> str | None:
    if x is None:
        return None
    key = str(x).strip().lower()
    return _CANON_SENTIMENT.get(key)


def transform_market(**_) -> dict:
    """raw.coingecko_market (JSONB, dados da Binance) -> staging.btc_market (tipado)."""
    eng = get_engine()
    raw = pd.read_sql("SELECT payload FROM raw.coingecko_market", eng)
    if raw.empty:
        logger.warning("[transform:market] raw vazio")
        return {"rows": 0}

    records = []
    for p in raw["payload"]:
        # psycopg2/sqlalchemy já entrega JSONB como dict
        d = p if isinstance(p, dict) else __import__("json").loads(p)
        records.append(
            {
                "market_date": pd.to_datetime(d["market_date"]).date(),
                "open_usd": float(d["open"]),
                "high_usd": float(d["high"]),
                "low_usd": float(d["low"]),
                "close_usd": float(d["close"]),
                "volume_usd": float(d["volume"]),
                "market_cap_usd": float(d["market_cap"]),
            }
        )
    df = pd.DataFrame(records).drop_duplicates("market_date").sort_values("market_date")

    conn = get_psycopg2_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM staging.btc_market")
            for _, r in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO staging.btc_market
                        (market_date, open_usd, high_usd, low_usd, close_usd,
                         volume_usd, market_cap_usd)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (market_date) DO NOTHING
                    """,
                    [
                        r["market_date"], r["open_usd"], r["high_usd"], r["low_usd"],
                        r["close_usd"], r["volume_usd"], r["market_cap_usd"],
                    ],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(f"[transform:market] {inserted} dias em staging.btc_market")
    return {"rows": inserted}


def transform_sentiment(**_) -> dict:
    """raw.fear_greed (texto) -> staging.btc_sentiment (tipado e normalizado)."""
    eng = get_engine()
    raw = pd.read_sql(
        "SELECT market_date, value, value_classification FROM raw.fear_greed", eng
    )
    if raw.empty:
        logger.warning("[transform:sentiment] raw vazio")
        return {"rows": 0, "dropped": 0}

    raw["market_date"] = pd.to_datetime(raw["market_date"], errors="coerce").dt.date
    raw["fng_value"] = raw["value"].apply(_to_int)
    raw["fng_classification"] = raw["value_classification"].apply(_canon_sentiment)

    before = len(raw)
    # R3: descarta linhas sem data OU sem valor de sentimento (não aproveitáveis)
    clean = raw.dropna(subset=["market_date", "fng_value", "fng_classification"])
    clean = clean.drop_duplicates("market_date")
    dropped = before - len(clean)

    conn = get_psycopg2_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM staging.btc_sentiment")
            for _, r in clean.iterrows():
                cur.execute(
                    """
                    INSERT INTO staging.btc_sentiment
                        (market_date, fng_value, fng_classification)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (market_date) DO NOTHING
                    """,
                    [r["market_date"], int(r["fng_value"]), r["fng_classification"]],
                )
                inserted += 1
    finally:
        conn.close()
    logger.success(
        f"[transform:sentiment] {inserted} dias em staging.btc_sentiment "
        f"({dropped} linhas descartadas por nulos/inválidos)"
    )
    return {"rows": inserted, "dropped": dropped}


# -----------------------------------------------------------------------------
# Funções puras de regra de negócio (testadas em /tests)
# -----------------------------------------------------------------------------

def daily_return_pct(open_usd: float, close_usd: float) -> float | None:
    """Variação percentual do dia: (close - open) / open * 100."""
    if open_usd in (None, 0):
        return None
    return round((close_usd - open_usd) / open_usd * 100, 6)


def intraday_range_pct(high_usd: float, low_usd: float) -> float | None:
    """Amplitude intradiária: (high - low) / low * 100."""
    if low_usd in (None, 0):
        return None
    return round((high_usd - low_usd) / low_usd * 100, 6)


if __name__ == "__main__":
    print(transform_market())
    print(transform_sentiment())
