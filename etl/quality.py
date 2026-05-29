"""
quality.py — Validações de qualidade de dados (requisito 7: mínimo 3 regras).

Implementamos 6 checagens, cobrindo as 4 categorias citadas no briefing:
  1. NULOS        -> staging.btc_market sem close nulo
  2. NULOS        -> staging.btc_sentiment sem fng_value nulo
  3. DUPLICIDADE  -> market_date único em staging.btc_market
  4. RANGE        -> fng_value entre 0 e 100
  5. RANGE        -> high >= low e high >= 0 (sanidade de preço)
  6. INTEGRIDADE  -> todo fato referencia uma dim_date e dim_sentiment válidas

Cada resultado é persistido em mart.dq_results (observabilidade).
Política: as checagens de STAGING são bloqueantes — se uma falhar, a task
levanta exceção e a DAG para (fail-fast), evitando carregar lixo no mart.
As checagens pós-carga (integridade) são auditadas mas não re-bloqueiam.
"""

from __future__ import annotations

import uuid

from loguru import logger
from sqlalchemy import text

from etl.db import get_engine


def _record(conn, run_id, check_name, layer, table_name, passed, n_violations, details=""):
    conn.execute(
        text(
            """
            INSERT INTO mart.dq_results
                (run_id, check_name, layer, table_name, passed, n_violations, details)
            VALUES (:run_id, :check_name, :layer, :table_name, :passed, :n, :details)
            """
        ),
        {
            "run_id": run_id, "check_name": check_name, "layer": layer,
            "table_name": table_name, "passed": passed, "n": n_violations,
            "details": details,
        },
    )


def _scalar(conn, sql: str) -> int:
    return int(conn.execute(text(sql)).scalar() or 0)


def validate_staging(**_) -> dict:
    """Roda as 5 checagens de staging. Bloqueante (fail-fast)."""
    run_id = uuid.uuid4().hex[:12]
    eng = get_engine()
    results = []
    with eng.begin() as conn:
        # 1. nulos em close
        v = _scalar(conn, "SELECT count(*) FROM staging.btc_market WHERE close_usd IS NULL")
        results.append(("not_null_close", "staging", "btc_market", v == 0, v))

        # 2. nulos em fng_value
        v = _scalar(conn, "SELECT count(*) FROM staging.btc_sentiment WHERE fng_value IS NULL")
        results.append(("not_null_fng_value", "staging", "btc_sentiment", v == 0, v))

        # 3. duplicidade de data
        v = _scalar(
            conn,
            "SELECT count(*) FROM (SELECT market_date FROM staging.btc_market "
            "GROUP BY market_date HAVING count(*) > 1) d",
        )
        results.append(("unique_market_date", "staging", "btc_market", v == 0, v))

        # 4. range do fng_value (0..100)
        v = _scalar(
            conn,
            "SELECT count(*) FROM staging.btc_sentiment "
            "WHERE fng_value < 0 OR fng_value > 100",
        )
        results.append(("range_fng_0_100", "staging", "btc_sentiment", v == 0, v))

        # 5. sanidade de preço (high >= low e valores positivos)
        v = _scalar(
            conn,
            "SELECT count(*) FROM staging.btc_market "
            "WHERE high_usd < low_usd OR low_usd <= 0 OR close_usd <= 0",
        )
        results.append(("price_sanity", "staging", "btc_market", v == 0, v))

        for name, layer, tbl, passed, n in results:
            _record(conn, run_id, name, layer, tbl, passed, n)

    failed = [r for r in results if not r[3]]
    for name, _, tbl, passed, n in results:
        status = "OK" if passed else f"FALHOU ({n} violações)"
        logger.info(f"[quality] {name} @ {tbl}: {status}")

    if failed:
        msg = ", ".join(f"{r[0]}={r[4]}" for r in failed)
        logger.error(f"[quality] checagens de staging FALHARAM: {msg}")
        raise ValueError(f"Data Quality bloqueou o pipeline: {msg}")

    logger.success(f"[quality] todas as 5 checagens de staging passaram (run={run_id})")
    return {"run_id": run_id, "checks": len(results), "failed": 0}


def validate_mart(**_) -> dict:
    """Checagem de integridade referencial pós-carga (auditoria)."""
    run_id = uuid.uuid4().hex[:12]
    eng = get_engine()
    with eng.begin() as conn:
        orphan_date = _scalar(
            conn,
            "SELECT count(*) FROM mart.fact_btc_daily f "
            "LEFT JOIN mart.dim_date d ON f.date_key = d.date_key "
            "WHERE d.date_key IS NULL",
        )
        orphan_sent = _scalar(
            conn,
            "SELECT count(*) FROM mart.fact_btc_daily f "
            "WHERE f.sentiment_key IS NOT NULL AND f.sentiment_key NOT IN "
            "(SELECT sentiment_key FROM mart.dim_sentiment)",
        )
        _record(conn, run_id, "fk_fact_dim_date", "mart", "fact_btc_daily",
                 orphan_date == 0, orphan_date)
        _record(conn, run_id, "fk_fact_dim_sentiment", "mart", "fact_btc_daily",
                 orphan_sent == 0, orphan_sent)

    logger.success(
        f"[quality] integridade do mart: date_orphans={orphan_date}, "
        f"sentiment_orphans={orphan_sent}"
    )
    return {"run_id": run_id, "orphan_date": orphan_date, "orphan_sentiment": orphan_sent}


if __name__ == "__main__":
    print(validate_staging())
    print(validate_mart())
