"""
db.py — Helpers de conexão com o PostgreSQL e bootstrap do schema.

Usa SQLAlchemy (engine) para pandas.to_sql e psycopg2 para DDL/DML manual.
"""

from __future__ import annotations

from pathlib import Path

import psycopg2
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.config import PG, PATHS


def get_engine() -> Engine:
    """Engine SQLAlchemy — usado por pandas e por queries de leitura."""
    return create_engine(PG.sqlalchemy_url, pool_pre_ping=True)


def get_psycopg2_conn():
    """Conexão psycopg2 crua — usada para execução de scripts SQL multi-statement."""
    return psycopg2.connect(
        host=PG.host, port=PG.port, dbname=PG.db, user=PG.user, password=PG.password
    )


def run_sql_file(path: str) -> None:
    """Executa um arquivo .sql (pode conter múltiplos statements)."""
    sql = Path(path).read_text(encoding="utf-8")
    conn = get_psycopg2_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
        logger.success(f"[db] script aplicado: {path}")
    finally:
        conn.close()


def bootstrap_schema(
    schema_path: str | None = None, seed_path: str | None = None
) -> dict:
    """
    Cria os schemas/tabelas (idempotente) e popula a dim_sentiment.
    É a primeira task da DAG.
    """
    schema_path = schema_path or PATHS.sql_schema
    seed_path = seed_path or PATHS.sql_seed
    run_sql_file(schema_path)
    run_sql_file(seed_path)
    return {"schema": schema_path, "seed": seed_path}


if __name__ == "__main__":
    print(bootstrap_schema())
