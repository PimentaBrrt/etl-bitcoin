# =====================================================================
# Makefile — ETL Bitcoin · ESPM Data Integration 2026.1
# =====================================================================
.PHONY: help up down logs ui run-dag demo test clean rebuild status shell psql

help:                ## Mostra esta ajuda
	@echo "Comandos disponiveis:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up:                  ## Sobe Postgres + Airflow (primeira vez demora ~2min)
	docker compose up -d --build
	@echo ""
	@echo "Airflow subindo. Aguarde ~40s e abra: http://localhost:8080"
	@echo "Usuario: admin  Senha: admin"

down:                ## Para os containers
	docker compose down

logs:                ## Tail dos logs do Airflow
	docker compose logs -f airflow-webserver airflow-scheduler

ui:                  ## Abre a UI do Airflow no navegador
	@( command -v xdg-open >/dev/null && xdg-open http://localhost:8080 ) || \
	  ( command -v open >/dev/null && open http://localhost:8080 ) || \
	  echo "Acesse manualmente: http://localhost:8080"

run-dag:             ## Dispara a DAG btc_etl_pipeline
	docker compose exec airflow-webserver airflow dags unpause btc_etl_pipeline || true
	docker compose exec airflow-webserver airflow dags trigger btc_etl_pipeline
	@echo "DAG disparada. Veja em http://localhost:8080/dags/btc_etl_pipeline/grid"

demo:                ## Roda o pipeline localmente sem Airflow (script unico)
	docker compose exec airflow-webserver python /opt/airflow/demo_run.py

test:                ## Roda os testes pytest
	docker compose exec airflow-webserver python -m pytest /opt/airflow/etl/../tests -q || \
	  docker compose run --rm --entrypoint python airflow-webserver -m pytest tests -q

psql:                ## Abre o psql no Data Warehouse
	docker compose exec postgres psql -U btc -d btc_dw

shell:               ## Abre shell no container do Airflow
	docker compose exec airflow-webserver bash

status:              ## Status dos containers
	docker compose ps

rebuild:             ## Rebuild da imagem sem cache
	docker compose build --no-cache

clean:               ## Para tudo e remove volumes (apaga o banco!)
	docker compose down -v
	@echo "Tudo limpo. Use 'make up' para comecar de novo."
