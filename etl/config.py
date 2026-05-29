"""
config.py — Configuração centralizada do pipeline.

Toda a parametrização (credenciais, caminhos, parâmetros de extração) é lida de
variáveis de ambiente, com defaults sensatos. Isso atende ao requisito
"nice-to-have" de uso de .env / sem hardcode de credenciais.

NUNCA faça commit do arquivo .env real — apenas do .env.example.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _get(key: str, default: str | None = None) -> str | None:
    val = os.getenv(key)
    return val if val not in (None, "") else default


@dataclass
class PostgresConfig:
    host: str = field(default_factory=lambda: _get("POSTGRES_HOST", "postgres"))
    port: int = field(default_factory=lambda: int(_get("POSTGRES_PORT", "5432")))
    db: str = field(default_factory=lambda: _get("POSTGRES_DB", "btc_dw"))
    user: str = field(default_factory=lambda: _get("POSTGRES_USER", "btc"))
    password: str = field(default_factory=lambda: _get("POSTGRES_PASSWORD", "btc"))

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


@dataclass
class CoinGeckoConfig:
    # LEGADO: a fonte de mercado ativa e a Binance (MarketConfig/MK abaixo).
    # Esta classe foi mantida apenas por compatibilidade e nao e mais usada.
    # Root da API (sobrescrevível por env). Default = endpoint público.
    base_url: str = field(
        default_factory=lambda: _get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3/")
    )
    # Chave demo (header x-cg-demo-api-key). Vem do .env em produção.
    api_key: str | None = field(default_factory=lambda: _get("COINGECKO_API_KEY"))
    coin_id: str = field(default_factory=lambda: _get("COINGECKO_COIN_ID", "bitcoin"))
    vs_currency: str = field(default_factory=lambda: _get("COINGECKO_VS_CURRENCY", "usd"))
    # Quantos dias de histórico buscar a cada execução (plano demo limita a 365).
    days: int = field(default_factory=lambda: int(_get("COINGECKO_DAYS", "365")))


@dataclass
class MarketConfig:
    """
    Fonte de mercado (Binance klines). Substitui a CoinGecko porque o plano
    gratuito da CoinGecko só cobre 365 dias, sem interseção com o CSV de
    sentimento (2018–2023).
    """
    base_url: str = field(default_factory=lambda: _get("MARKET_BASE_URL", "https://api.binance.com"))
    symbol: str = field(default_factory=lambda: _get("MARKET_SYMBOL", "BTCUSDT"))
    # Janela alinhada ao CSV do Fear & Greed para garantir interseção no join.
    start_date: str = field(default_factory=lambda: _get("MARKET_START_DATE", "2018-02-01"))
    end_date: str = field(default_factory=lambda: _get("MARKET_END_DATE", "2023-03-31"))
    # Supply circulante aproximado do BTC, usado para estimar market cap.
    circulating_supply: float = field(
        default_factory=lambda: float(_get("MARKET_CIRCULATING_SUPPLY", "19800000"))
    )


@dataclass
class PathsConfig:
    # Caminho do CSV do Kaggle (Fear & Greed) montado dentro do container.
    seed_csv: str = field(
        default_factory=lambda: _get(
            "FNG_CSV_PATH", "/opt/airflow/data/seeds/fear_and_greed.csv"
        )
    )
    sql_schema: str = field(
        default_factory=lambda: _get("SQL_SCHEMA_PATH", "/opt/airflow/sql/schema.sql")
    )
    sql_seed: str = field(
        default_factory=lambda: _get(
            "SQL_SEED_PATH", "/opt/airflow/sql/seed_dim_sentiment.sql"
        )
    )


# Instâncias prontas para importar
PG = PostgresConfig()
CG = CoinGeckoConfig()
MK = MarketConfig()
PATHS = PathsConfig()
