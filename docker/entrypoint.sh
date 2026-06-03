#!/usr/bin/env bash
# =============================================================================
# entrypoint.sh — espera o Postgres e DELEGA ao entrypoint oficial do Airflow.
#
# Por que existe:
#   Ao dar "docker compose up" depois de deixar o ambiente parado, o Postgres
#   pode levar alguns segundos para aceitar conexoes. Sem esta espera, o
#   Airflow tenta migrar/conectar cedo demais e falha.
#
# Importante:
#   NAO substituimos o entrypoint oficial da imagem. Apos a espera, chamamos
#   "/entrypoint" (script oficial do apache/airflow), que e quem processa as
#   variaveis _AIRFLOW_DB_MIGRATE e _AIRFLOW_WWW_USER_* (criacao do admin).
# =============================================================================
set -e

HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DBUSER="${POSTGRES_USER:-btc}"
RETRIES=30
WAIT=2

echo "[entrypoint] Aguardando Postgres em ${HOST}:${PORT} ..."
for i in $(seq 1 "$RETRIES"); do
    if pg_isready -h "$HOST" -p "$PORT" -U "$DBUSER" -q; then
        echo "[entrypoint] Postgres pronto (tentativa $i)."
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        echo "[entrypoint] ERRO: Postgres indisponivel apos $((RETRIES * WAIT))s."
        exit 1
    fi
    echo "[entrypoint] Tentativa $i/$RETRIES — aguardando ${WAIT}s..."
    sleep "$WAIT"
done

# Se o comando for "webserver", remove um PID file orfao deixado por um
# container anterior que nao encerrou de forma limpa. Sem isto, o webserver
# aborta com "Already running on PID ... (or pid file is stale)".
if [ "$1" = "webserver" ]; then
    PIDFILE="${AIRFLOW_HOME:-/opt/airflow}/airflow-webserver.pid"
    if [ -f "$PIDFILE" ]; then
        echo "[entrypoint] Removendo PID file orfao: $PIDFILE"
        rm -f "$PIDFILE"
    fi
fi

# Delega ao entrypoint OFICIAL da imagem do Airflow, repassando o comando.
exec /entrypoint "$@"
