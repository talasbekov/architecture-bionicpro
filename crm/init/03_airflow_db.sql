-- Отдельная база для метаданных Airflow, чтобы не поднимать ещё один Postgres.
CREATE DATABASE airflow OWNER crm_user;
