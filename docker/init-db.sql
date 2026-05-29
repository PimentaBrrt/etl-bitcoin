-- Roda automaticamente na primeira inicialização do Postgres.
-- O banco btc_dw (Data Warehouse) já é criado pela env POSTGRES_DB.
-- Aqui criamos o banco de metadados do Airflow.
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO btc;
