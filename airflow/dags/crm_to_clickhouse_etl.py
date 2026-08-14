"""ETL из CRM (PostgreSQL) в ClickHouse и пересчёт витрины отчётности BionicPRO.

Схема работы:
  extract_crm_clients  ─┐
                        ├─> build_report_mart ─> update_watermark
  extract_crm_prostheses┘

Справочники CRM переливаются целиком (объём маленький), телеметрия уже лежит
в ClickHouse, DAG только считает по ней суточные агрегаты.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Окно пересчёта витрины: сколько последних суток переcчитываем на каждом запуске.
# Нужно, чтобы подхватить телеметрию, приехавшую с опозданием.
MART_WINDOW_DAYS = int(os.getenv("MART_WINDOW_DAYS", "7"))

MART_NAME = "user_report_mart"

# Параметры подключений. Airflow Connection используем, если он заведён,
# иначе берём переменные окружения — так DAG работает сразу после подъёма compose.
CRM_CONN_ID = "crm_postgres"
CRM_DEFAULTS = {
    "host": os.getenv("CRM_DB_HOST", "crm_db"),
    "port": int(os.getenv("CRM_DB_PORT", "5432")),
    "dbname": os.getenv("CRM_DB_NAME", "crm"),
    "user": os.getenv("CRM_DB_USER", "crm_user"),
    "password": os.getenv("CRM_DB_PASSWORD", "crm_password"),
}

CLICKHOUSE = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "port": int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    "database": os.getenv("CLICKHOUSE_DB", "bionicpro"),
    "username": os.getenv("CLICKHOUSE_USER", "analytics"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "analytics_password"),
}


def get_clickhouse_client():
    return clickhouse_connect.get_client(**CLICKHOUSE)


def fetch_from_crm(query: str) -> list[tuple]:
    """Читает данные из CRM. Сначала пробует Airflow Connection, потом psycopg2 напрямую."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CRM_CONN_ID)
        return hook.get_records(query)
    except Exception as exc:  # коннекта нет или провайдер не установлен
        log.warning("Connection %s недоступен (%s), подключаюсь по параметрам из env", CRM_CONN_ID, exc)

    import psycopg2

    conn = psycopg2.connect(**CRM_DEFAULTS)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    finally:
        conn.close()


def _as_naive_dt(value) -> datetime:
    """ClickHouse DateTime хранится без таймзоны — приводим TIMESTAMPTZ из Postgres к naive UTC."""
    if value is None:
        return datetime(1970, 1, 1)
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def extract_crm_clients(**_) -> None:
    rows = fetch_from_crm(
        "SELECT user_id, id, full_name, COALESCE(email, ''), COALESCE(country, ''), updated_at "
        "FROM public.clients"
    )
    data = [
        (r[0], int(r[1]), r[2], r[3], r[4], _as_naive_dt(r[5]))
        for r in rows
    ]
    if not data:
        log.warning("В CRM нет клиентов")
        return

    client = get_clickhouse_client()
    # ReplacingMergeTree сам схлопнет старые версии по client_id, чистить таблицу не нужно
    client.insert(
        "crm_clients",
        data,
        column_names=["user_id", "client_id", "full_name", "email", "country", "updated_at"],
    )
    log.info("Загружено клиентов: %s", len(data))


def extract_crm_prostheses(**_) -> None:
    rows = fetch_from_crm(
        "SELECT id, client_id, serial_number, COALESCE(model, ''), COALESCE(status, ''), updated_at "
        "FROM public.prostheses"
    )
    data = [
        (int(r[0]), int(r[1]), r[2], r[3], r[4], _as_naive_dt(r[5]))
        for r in rows
    ]
    if not data:
        log.warning("В CRM нет протезов")
        return

    client = get_clickhouse_client()
    client.insert(
        "crm_prostheses",
        data,
        column_names=["id", "client_id", "serial_number", "model", "status", "updated_at"],
    )
    log.info("Загружено протезов: %s", len(data))


def _window(context) -> tuple[date, date]:
    """Окно пересчёта: последние MART_WINDOW_DAYS суток, заканчивая вчерашним днём."""
    run_day = context["data_interval_end"].date()
    date_to = run_day - timedelta(days=1)
    date_from = date_to - timedelta(days=MART_WINDOW_DAYS - 1)
    return date_from, date_to


def build_report_mart(**context) -> None:
    date_from, date_to = _window(context)
    client = get_clickhouse_client()

    # Идемпотентность: перед вставкой физически удаляем строки за пересчитываемое окно.
    # Одного ReplacingMergeTree мало — если у протеза за день не осталось телеметрии,
    # старая строка иначе зависла бы в витрине навсегда.
    client.command(
        f"ALTER TABLE {MART_NAME} DELETE "
        f"WHERE report_date BETWEEN toDate('{date_from}') AND toDate('{date_to}')",
        settings={"mutations_sync": 2},
    )

    sql = (SQL_DIR / "build_report_mart.sql").read_text(encoding="utf-8")
    client.command(sql.format(date_from=date_from, date_to=date_to))

    rows = client.command(
        f"SELECT count() FROM {MART_NAME} "
        f"WHERE report_date BETWEEN toDate('{date_from}') AND toDate('{date_to}')"
    )
    log.info("Витрина пересчитана за %s..%s, строк в окне: %s", date_from, date_to, rows)


def update_watermark(**context) -> None:
    """Отмечаем дату, по которую витрина посчитана целиком (вчерашние сутки закрыты)."""
    _, date_to = _window(context)
    client = get_clickhouse_client()
    client.insert(
        "etl_watermark",
        [(MART_NAME, date_to, datetime.utcnow().replace(microsecond=0))],
        column_names=["mart", "processed_until", "updated_at"],
    )
    log.info("Watermark для %s: %s", MART_NAME, date_to)


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crm_to_clickhouse_etl",
    description="Перелив справочников CRM в ClickHouse и пересчёт витрины user_report_mart",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bionicpro", "etl", "clickhouse"],
) as dag:
    extract_clients = PythonOperator(
        task_id="extract_crm_clients",
        python_callable=extract_crm_clients,
    )

    extract_prostheses = PythonOperator(
        task_id="extract_crm_prostheses",
        python_callable=extract_crm_prostheses,
    )

    build_mart = PythonOperator(
        task_id="build_report_mart",
        python_callable=build_report_mart,
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    [extract_clients, extract_prostheses] >> build_mart >> watermark
