"""
Pacote ETL — Pipeline de Integração de Dados do Bitcoin.

Módulos (ordem do pipeline):
    config     -> configuração centralizada (lê variáveis de ambiente / .env)
    db         -> helpers de conexão e bootstrap do schema Postgres
    extract    -> extração das 2 fontes (API Binance + CSV Kaggle)
    transform  -> limpeza, tipagem e regras de negócio
    quality    -> validações de qualidade de dados (data quality checks)
    load       -> carga na modelagem dimensional (star schema)
"""

__version__ = "1.0.0"
