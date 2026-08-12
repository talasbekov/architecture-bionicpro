import logging
from datetime import date
from typing import Optional

import clickhouse_connect

from .config import settings

log = logging.getLogger(__name__)


def _client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def processed_until() -> Optional[date]:
    """До какой даты витрина посчитана. Дальше этой границы отчёт строить нельзя:
    данных в OLAP ещё нет."""
    query = """
        SELECT max(processed_until) AS processed_until
        FROM etl_watermark FINAL
        WHERE mart = %(mart)s
    """
    with _client() as client:
        rows = client.query(query, parameters={"mart": settings.mart_table}).result_rows
    if not rows or rows[0][0] is None:
        return None
    value = rows[0][0]
    return value if isinstance(value, date) else None


def fetch_report_rows(user_id: str, date_from: date, date_to: date) -> list[dict]:
    """Витрина отсортирована по (user_id, report_date, serial_number),
    поэтому выборка по одному пользователю читает минимум гранул."""
    query = f"""
        SELECT
            report_date,
            serial_number,
            model,
            full_name,
            sessions,
            gestures_total,
            avg_latency_ms,
            max_latency_ms,
            p95_latency_ms,
            avg_battery,
            min_battery,
            errors
        FROM {settings.mart_table} FINAL
        WHERE user_id = %(user_id)s
          AND report_date BETWEEN %(date_from)s AND %(date_to)s
        ORDER BY report_date, serial_number
    """
    with _client() as client:
        result = client.query(
            query,
            parameters={"user_id": user_id, "date_from": date_from, "date_to": date_to},
        )
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]
