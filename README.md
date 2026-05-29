# Estudo Financeiro e Comportamental do Bitcoin — Pipeline ETL End-to-End

> Trabalho Semestral · **Data Integration** · Sistemas de Informação · ESPM · 2026.1
> Prof. Me. Andre Insardi

Pipeline de integração de dados que cruza o **preço de mercado do Bitcoin** (API
REST da Binance) com o **índice de medo e ganância** dos investidores (dataset
Kaggle), para investigar a relação entre o *sentimento do mercado* e a *variação
de preço* do ativo.

---

## Integrantes do Grupo

| Nome | RA |
|------|----|
| Arthur Borba Ferreira | 245559 |
| Enzo De Marchi Malagoli | 243089 |
| Luis Felipe Galina Degaspari | 238000 |
| Luiz Felipe Pimenta Berrettini | 245388 |
| Luiz Fernando Pazdziora Costa | 228615 |

---

## Objetivo e Pergunta de Negócio

O Bitcoin é notoriamente volátil e fortemente influenciado por psicologia de
mercado. Este projeto integra duas fontes para responder a perguntas como:

- Quando o mercado está **ganancioso**, o Bitcoin realmente sobe mais — ou é aí
  que ele costuma corrigir?
- O **medo extremo** vem acompanhado de **picos de volume** (pânico de venda)?
- Como o **humor predominante** se distribuiu ano a ano?

O resultado é apresentável a um stakeholder não-técnico (área de investimentos,
risco ou comunicação), atendendo ao critério de relevância de negócio.

---

## Fontes de Dados (heterogêneas)

| # | Fonte | Tipo | O que extraímos |
|---|-------|------|-----------------|
| 1 | **API REST Binance** (`/api/v3/klines`) | API REST (estruturada) | Abertura, fechamento, máxima, mínima e **volume** diários (OHLCV); o **market cap** é estimado a partir do preço |
| 2 | **Kaggle — Bitcoin and Fear and Greed** | CSV (semi-estruturado / "sujo") | Colunas `Value` (índice 0–100) e `Value_Classification` (Extreme Fear … Extreme Greed) |

- Fonte 1: <https://api.binance.com> — endpoint `GET /api/v3/klines` (público, sem chave)
- Fonte 2: <https://www.kaggle.com/datasets/adilbhatti/bitcoin-and-fear-and-greed>

> **Mudança de fonte de mercado (CoinGecko → Binance).** A versão inicial usava
> a API da CoinGecko. Trocamos pela Binance porque o **plano gratuito da CoinGecko
> só fornece dados diários dos últimos 365 dias**, enquanto o CSV de Fear & Greed
> cobre **2018-02 a 2023-03**. Não havia **nenhuma interseção de datas** entre as
> duas fontes, então o cruzamento sentimento × preço resultava vazio. A Binance
> (`/api/v3/klines`) fornece OHLCV diário **desde 2017, de graça e sem chave**,
> permitindo casar a janela de mercado com o período do CSV. Detalhes e o que isso
> implica estão na seção [Decisão de Engenharia](#-decisão-de-engenharia-troca-da-fonte-de-mercado).

O CSV do Kaggle entra **cru** (todas as colunas como texto, contendo nulos) e é a
fonte semi-estruturada exigida pelo briefing — a camada de transformação faz a
limpeza, evidenciando os problemas de qualidade do dado bruto.

---

## Arquitetura

Pipeline orquestrado pelo **Apache Airflow**, com persistência em **PostgreSQL**
organizada em três camadas (estilo *medallion*): `raw → staging → mart`.

Diagrama completo (Mermaid + ERD): [`docs/architecture.md`](docs/architecture.md).

```
Fontes            Airflow (DAG)                         Postgres
─────────         ─────────────────────────────         ───────────────────────
Binance    ─► extract_coingecko ─► transform_market ─┐
                                                     ├─► validate_staging ─► load_dim_date
Kaggle CSV ─► extract_kaggle_fng ─► transform_sent. ─┘        (DQ)              └─► load_fact ─► validate_mart
                                                                                       │
                                                                              raw → staging → mart (star schema)
```

> Nota: a task ainda se chama `extract_coingecko` (e a tabela `raw.coingecko_market`)
> por compatibilidade — apenas a fonte interna passou a ser a Binance. Optamos por
> não renomear identificadores para não alterar a lógica já validada do pipeline.

### Modelagem Dimensional (Star Schema)

- **Fato** `mart.fact_btc_daily` — grão = **1 dia** de mercado.
  Métricas: OHLC, volume, market cap, `daily_return_pct`, `intraday_range_pct`,
  `fng_value`.
- **Dimensão 1** `mart.dim_date` — calendário (ano, trimestre, mês, dia da semana…).
- **Dimensão 2** `mart.dim_sentiment` — as 5 classes do Fear & Greed Index, com
  ordem e agrupamento (Fear / Neutral / Greed).

---

## Decisão de Engenharia: troca da fonte de mercado

**O que mudou.** A fonte de preço do Bitcoin passou de **CoinGecko** para
**Binance** (`/api/v3/klines`).

**Por quê.** Durante a integração, as 3 consultas de valor retornavam vazias: o
join entre preço e sentimento não casava nenhuma linha. A causa foi a falta de
interseção temporal entre as fontes:

| Fonte | Cobertura disponível (grátis) |
|-------|-------------------------------|
| CSV Kaggle Fear & Greed | 2018-02-01 → 2023-03-31 |
| CoinGecko (plano gratuito) | apenas os últimos **365 dias** (2025–2026) |

A [documentação oficial da CoinGecko](https://support.coingecko.com/hc/en-us/articles/4538747001881)
confirma que o plano público limita o histórico diário aos últimos 365 dias;
acessar 2018–2023 exigiria um plano pago. Como o CSV de sentimento termina em
2023, **não havia um único dia em comum** — daí o resultado vazio.

A Binance expõe candles diários (OHLCV) **desde 2017, de graça e sem chave de
API**, o que permite buscar exatamente a janela do CSV (`2018-02-01` a
`2023-03-31`, parametrizada em `MARKET_START_DATE` / `MARKET_END_DATE`) e garantir
a interseção necessária para o cruzamento.

**O que NÃO mudou (decisão consciente).** Para não alterar a lógica já validada,
mantivemos os identificadores internos: a task continua `extract_coingecko`, a
tabela `raw.coingecko_market` e o formato do payload são os mesmos. Apenas o
*conteúdo* da extração passou a vir da Binance. Assim, `transform`, `load`, o
schema e as 3 consultas seguem idênticos.

**Ressalvas honestas da nova fonte** (relevantes para a leitura dos resultados):

- A Binance **não fornece market cap**. Ele é **estimado** como
  `preço de fechamento × supply circulante` (constante `MARKET_CIRCULATING_SUPPLY`,
  ~19,8 mi BTC). É uma aproximação: o supply real cresce lentamente no tempo, então
  o market cap histórico tem um pequeno viés. Para as perguntas deste estudo
  (comportamento × preço/volume) isso não compromete as conclusões.
- O **volume** usado é o *quote volume* em USDT (≈ USD), e os preços são do par
  `BTCUSDT` — padrão de mercado, mas conceitualmente distinto de um preço médio
  global multi-exchange como o da CoinGecko.

---

## Estrutura do Repositório

```
btc_pipeline/
├── dags/
│   └── btc_etl_pipeline.py      # DAG do Airflow (8 tasks, @daily)
├── etl/                          # pacote Python do ETL
│   ├── config.py                 # configuração via env / .env (sem hardcode)
│   ├── db.py                     # conexão Postgres + bootstrap do schema
│   ├── extract.py                # extração das 2 fontes (API + CSV)
│   ├── transform.py              # limpeza, tipagem e regras de negócio
│   ├── quality.py                # 5 validações de DQ (fail-fast) + integridade
│   └── load.py                   # carga no star schema (UPSERT incremental)
├── sql/
│   ├── schema.sql                # DDL das 3 camadas + tabela de DQ
│   ├── seed_dim_sentiment.sql    # carga da dimensão de sentimento
│   └── analytics_queries.sql     # as 3 consultas de valor
├── tests/
│   └── test_transform.py         # testes pytest das funções de transformação
├── docs/
│   └── architecture.md           # diagramas Mermaid (fluxo + ERD)
├── data/seeds/
│   └── fear_and_greed.csv         # dataset Kaggle (fonte 2)
├── docker/
│   └── init-db.sql               # cria o banco de metadados do Airflow
├── demo_run.py                   # roda o pipeline inteiro fora do Airflow
├── Dockerfile                    # imagem Airflow + deps
├── docker-compose.yml            # Postgres + Airflow
├── requirements.txt
├── Makefile                      # atalhos (up, run-dag, test, psql…)
├── .env.example                  # modelo de variáveis de ambiente
└── .gitignore
```

---

## Stack

| Camada | Ferramenta | Versão |
|--------|-----------|--------|
| Linguagem | Python | 3.11 |
| Conteinerização | Docker + Docker Compose | 24+ |
| Orquestração | Apache Airflow | 2.9 |
| Banco de Dados | PostgreSQL | 15 |
| Bibliotecas | pandas, requests, sqlalchemy, psycopg2, tenacity, loguru, pytest | — |

---

## Como Executar (passo-a-passo)

### Pré-requisitos
- Docker e Docker Compose instalados.

### 1. Clone e configure as credenciais

```bash
git clone <URL_DO_SEU_REPO>.git
cd btc_pipeline
cp .env.example .env       # já vem pronto; a Binance não exige chave de API
```

O `.env` traz os parâmetros da fonte de mercado (`MARKET_*`) e do Postgres. A
**Binance é pública e não requer chave**, então não há segredo a preencher para a
fonte 1. Ainda assim, **não comite o `.env`** — ele já está no `.gitignore`.

### 2. Suba o ambiente

```bash
docker compose up -d --build      # ou: make up
```

Aguarde ~40s. A primeira execução baixa as imagens e instala dependências.

### 3. Acesse o Airflow

Abra <http://localhost:8080> · usuário **admin** · senha **admin**.

### 4. Rode o pipeline

Pela UI: ative a DAG `btc_etl_pipeline` e clique em ▶ (*Trigger DAG*).

Ou pelo terminal:

```bash
make run-dag
```

### 5. Veja os resultados

```bash
make psql      # abre o psql no Data Warehouse (btc_dw)
```

```sql
\i /opt/airflow/sql/analytics_queries.sql   -- roda as 3 consultas de valor
```

Ou pelo terminal (psql dentro do container):

```bash
docker exec -i btc_postgres psql -U btc -d btc_dw < sql/analytics_queries.sql
```

### Atalho: rodar tudo fora do Airflow (útil na demo)

```bash
make demo      # executa o pipeline em sequência e imprime as 3 consultas
```

---

## Validações de Qualidade de Dados

`etl/quality.py` implementa **5 checagens bloqueantes** em `staging` (a DAG para
se alguma falhar — *fail-fast*) e **2 de integridade** pós-carga. Cobrem as
categorias pedidas no briefing:

| # | Checagem | Categoria | Tabela |
|---|----------|-----------|--------|
| 1 | `not_null_close` | Nulos | staging.btc_market |
| 2 | `not_null_fng_value` | Nulos | staging.btc_sentiment |
| 3 | `unique_market_date` | Duplicidade | staging.btc_market |
| 4 | `range_fng_0_100` | Range | staging.btc_sentiment |
| 5 | `price_sanity` (high ≥ low, valores > 0) | Range / consistência | staging.btc_market |
| 6 | `fk_fact_dim_date` | Integridade referencial | mart.fact_btc_daily |
| 7 | `fk_fact_dim_sentiment` | Integridade referencial | mart.fact_btc_daily |

Todos os resultados ficam registrados em `mart.dq_results` (observabilidade).

> No dataset do Kaggle há **3 linhas com sentimento nulo** que são corretamente
> descartadas na transformação — exatamente o tipo de problema que a camada de DQ
> existe para tratar.

---

## Testes Automatizados (nice-to-have)

```bash
make test       # roda pytest dentro do container
```

`tests/test_transform.py` cobre as funções puras de transformação: cálculo de
retorno diário, amplitude intradiária, tipagem de inteiros e normalização das
classes de sentimento.

---

## Reprodutibilidade e Incrementalidade

- **Idempotência**: o `bootstrap_schema` usa `CREATE TABLE IF NOT EXISTS`; a carga
  do fato usa `INSERT … ON CONFLICT (date_key) DO UPDATE` (UPSERT), então
  reexecutar a DAG não duplica dados — só atualiza/insere os dias da janela.
- **Do zero em outra máquina**: `docker compose up` sobe Postgres + Airflow já
  configurados; o CSV do Kaggle vai versionado em `data/seeds/`, e a fonte de
  mercado (Binance) é pública e sem chave. Nenhum ajuste manual é necessário.

---

## As 3 Consultas de Valor

Estão em [`sql/analytics_queries.sql`](sql/analytics_queries.sql):

1. **Retorno médio e volatilidade por sentimento** — o humor do mercado se
   traduz em retorno?
2. **Volume e market cap por faixa do Fear & Greed** — medo gera pânico de volume?
3. **Resumo anual** — desempenho e humor predominante por ano.

---

## Declaração de Uso de Inteligência Artificial

- Usamos o **Claude** para estruturar o esqueleto do pipeline,
  revisar a arquitetura em camadas e sugerir as regras de qualidade de dados.
- Usamos o **GitHub Copilot** e o **Chat GPT** para acelerar a escrita de SQL, dos
  testes e de correções no código.
- Todo o código foi **revisado e compreendido** pelo grupo; somos responsáveis
  pelo que está entregue.

---

## Referências

- Binance API Documentation (Kline/Candlestick Data). Disponível em:
  <https://developers.binance.com/docs/binance-spot-api-docs>.
- CoinGecko API Documentation (fonte original, substituída). Disponível em:
  <https://docs.coingecko.com/>.
- BHATTI, A. *Bitcoin and Fear and Greed* [dataset]. Kaggle. Disponível em:
  <https://www.kaggle.com/datasets/adilbhatti/bitcoin-and-fear-and-greed>.
- Apache Airflow Documentation. Disponível em: <https://airflow.apache.org/docs/>.
- KIMBALL, R.; ROSS, M. *The Data Warehouse Toolkit*. 3. ed. Wiley, 2013.
