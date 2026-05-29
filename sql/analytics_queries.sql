-- =============================================================================
-- 3 Consultas de Valor — Estudo Financeiro e Comportamental do Bitcoin
-- Cada query responde a uma pergunta de negócio para um stakeholder.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CONSULTA 1
-- Pergunta: "O sentimento do mercado se traduz em retorno? Quando os
--            investidores estão gananciosos, o Bitcoin de fato sobe mais?"
-- Retorno médio diário e volatilidade por classe de sentimento.
-- -----------------------------------------------------------------------------
SELECT
    ds.classification                       AS sentimento,
    ds.sentiment_order                      AS ordem,
    COUNT(*)                                AS dias,
    ROUND(AVG(f.daily_return_pct), 4)       AS retorno_medio_pct,
    ROUND(STDDEV_POP(f.daily_return_pct), 4) AS volatilidade_pct,
    ROUND(AVG(f.intraday_range_pct), 4)     AS amplitude_media_pct
FROM mart.fact_btc_daily f
JOIN mart.dim_sentiment ds ON f.sentiment_key = ds.sentiment_key
GROUP BY ds.classification, ds.sentiment_order
ORDER BY ds.sentiment_order;


-- -----------------------------------------------------------------------------
-- CONSULTA 2
-- Pergunta: "Existe relação entre o nível do Fear & Greed Index e o volume
--            negociado? O medo extremo gera pânico de vendas (volume alto)?"
-- Volume e market cap médios por faixa do índice.
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN f.fng_value < 25 THEN '0-24  (Medo Extremo)'
        WHEN f.fng_value < 45 THEN '25-44 (Medo)'
        WHEN f.fng_value < 55 THEN '45-54 (Neutro)'
        WHEN f.fng_value < 75 THEN '55-74 (Ganância)'
        ELSE                       '75-100 (Ganância Extrema)'
    END                                       AS faixa_fng,
    COUNT(*)                                  AS dias,
    ROUND(AVG(f.volume_usd) / 1e9, 2)         AS volume_medio_bi_usd,
    ROUND(AVG(f.close_usd), 2)                AS preco_medio_usd,
    ROUND(AVG(f.market_cap_usd) / 1e9, 2)     AS market_cap_medio_bi_usd
FROM mart.fact_btc_daily f
WHERE f.fng_value IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- CONSULTA 3
-- Pergunta: "Olhando por ano, qual foi o desempenho do Bitcoin e em que
--            humor o mercado passou a maior parte do tempo?"
-- Resumo anual: preço de fim de ano, variação e sentimento predominante.
-- -----------------------------------------------------------------------------
SELECT
    dd.year                                   AS ano,
    COUNT(*)                                  AS dias_negociados,
    ROUND(MIN(f.low_usd), 2)                  AS minima_usd,
    ROUND(MAX(f.high_usd), 2)                 AS maxima_usd,
    ROUND(AVG(f.close_usd), 2)                AS preco_medio_usd,
    ROUND(AVG(f.fng_value), 1)                AS fng_medio,
    MODE() WITHIN GROUP (ORDER BY ds.classification) AS sentimento_predominante
FROM mart.fact_btc_daily f
JOIN mart.dim_date dd       ON f.date_key = dd.date_key
LEFT JOIN mart.dim_sentiment ds ON f.sentiment_key = ds.sentiment_key
GROUP BY dd.year
ORDER BY dd.year;
