-- =============================================================================
-- Seed da dimensão de sentimento (Fear & Greed Index)
-- Carregada uma vez; o fato referencia por classification.
-- =============================================================================

INSERT INTO mart.dim_sentiment (classification, sentiment_order, sentiment_group) VALUES
    ('Extreme Fear',  1, 'Fear'),
    ('Fear',          2, 'Fear'),
    ('Neutral',       3, 'Neutral'),
    ('Greed',         4, 'Greed'),
    ('Extreme Greed', 5, 'Greed')
ON CONFLICT (classification) DO NOTHING;
