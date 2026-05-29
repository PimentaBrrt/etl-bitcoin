-- =============================================================================
-- Schema do Data Warehouse — Projeto "Estudo Financeiro e Comportamental do BTC"
-- SGBD: PostgreSQL 15+
--
-- Arquitetura em camadas (estilo medallion):
--   raw      -> dados crus, exatamente como chegam das fontes (JSON / texto)
--   staging  -> dados achatados, tipados e limpos (1 linha por dia)
--   mart     -> modelagem dimensional (star schema): 1 fato + 2 dimensões
--
-- Modelagem dimensional escolhida (requisito 6 do briefing):
--   FATO:       mart.fact_btc_daily   (grão = 1 dia de mercado do Bitcoin)
--   DIMENSÃO 1: mart.dim_date         (calendário)
--   DIMENSÃO 2: mart.dim_sentiment    (classificação do Fear & Greed Index)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- -----------------------------------------------------------------------------
-- CAMADA RAW
-- -----------------------------------------------------------------------------

-- Fonte 1 (API REST Binance): payload JSON cru por dia de mercado.
-- (nome da tabela mantido como raw.coingecko_market p/ compatibilidade)
CREATE TABLE IF NOT EXISTS raw.coingecko_market (
    ingestion_ts TIMESTAMPTZ DEFAULT now(),
    market_date  DATE,
    payload      JSONB
);

-- Fonte 2 (CSV/dataset Kaggle Fear & Greed): linha crua como string.
CREATE TABLE IF NOT EXISTS raw.fear_greed (
    ingestion_ts          TIMESTAMPTZ DEFAULT now(),
    market_date           TEXT,
    value                 TEXT,
    value_classification  TEXT,
    btc_closing           TEXT,
    btc_volume            TEXT
);

-- -----------------------------------------------------------------------------
-- CAMADA STAGING (achatada, tipada, 1 linha por dia)
-- -----------------------------------------------------------------------------

-- Mercado vindo da Binance (OHLC + volume; market cap estimado)
CREATE TABLE IF NOT EXISTS staging.btc_market (
    market_date  DATE PRIMARY KEY,
    open_usd     NUMERIC(20, 4),
    high_usd     NUMERIC(20, 4),
    low_usd      NUMERIC(20, 4),
    close_usd    NUMERIC(20, 4),
    volume_usd   NUMERIC(24, 2),
    market_cap_usd NUMERIC(24, 2)
);

-- Sentimento vindo do dataset Kaggle (Fear & Greed)
CREATE TABLE IF NOT EXISTS staging.btc_sentiment (
    market_date          DATE PRIMARY KEY,
    fng_value            INTEGER,        -- 0..100
    fng_classification   VARCHAR(20)     -- Extreme Fear .. Extreme Greed
);

-- -----------------------------------------------------------------------------
-- CAMADA MART (star schema)
-- -----------------------------------------------------------------------------

-- DIMENSÃO 1 — Calendário
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key     INTEGER PRIMARY KEY,   -- AAAAMMDD
    full_date    DATE NOT NULL UNIQUE,
    year         INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   VARCHAR(15) NOT NULL,
    day          INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,      -- 0=domingo .. 6=sábado
    weekday_name VARCHAR(15) NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

-- DIMENSÃO 2 — Sentimento de mercado (Fear & Greed)
CREATE TABLE IF NOT EXISTS mart.dim_sentiment (
    sentiment_key   SERIAL PRIMARY KEY,
    classification  VARCHAR(20) NOT NULL UNIQUE,  -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    sentiment_order INTEGER NOT NULL,             -- 1 (mais medo) .. 5 (mais ganância)
    sentiment_group VARCHAR(10) NOT NULL          -- Fear | Neutral | Greed
);

-- FATO — Mercado diário do Bitcoin (grão: 1 dia)
CREATE TABLE IF NOT EXISTS mart.fact_btc_daily (
    fact_id         BIGSERIAL PRIMARY KEY,
    date_key        INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    sentiment_key   INTEGER REFERENCES mart.dim_sentiment(sentiment_key),
    -- métricas de preço (Binance)
    open_usd        NUMERIC(20, 4),
    high_usd        NUMERIC(20, 4),
    low_usd         NUMERIC(20, 4),
    close_usd       NUMERIC(20, 4),
    volume_usd      NUMERIC(24, 2),
    market_cap_usd  NUMERIC(24, 2),
    -- métricas derivadas (regras de negócio)
    daily_return_pct  NUMERIC(12, 6),  -- variação % no dia: (close-open)/open
    intraday_range_pct NUMERIC(12, 6), -- amplitude do dia: (high-low)/low
    -- métrica de sentimento (Kaggle)
    fng_value       INTEGER,
    -- proveniência
    source_market    VARCHAR(20) DEFAULT 'binance',
    source_sentiment VARCHAR(20) DEFAULT 'kaggle_fng',
    loaded_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE (date_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_date      ON mart.fact_btc_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sentiment ON mart.fact_btc_daily(sentiment_key);

-- -----------------------------------------------------------------------------
-- TABELA DE OBSERVABILIDADE — resultado das validações de qualidade de dados
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dq_results (
    run_id      VARCHAR(40),
    run_ts      TIMESTAMPTZ DEFAULT now(),
    check_name  VARCHAR(60),   -- ex.: not_null_close, unique_date, range_fng
    layer       VARCHAR(20),   -- staging | mart
    table_name  VARCHAR(60),
    passed      BOOLEAN,
    n_violations INTEGER,
    details     TEXT
);
