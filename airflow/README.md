# Airflow: ETL CRM -> ClickHouse

## Что здесь

- `dags/crm_to_clickhouse_etl.py` — DAG `crm_to_clickhouse_etl`, запуск ежедневно в 02:00, `catchup=False`, 2 ретрая с паузой 5 минут.
- `sql/build_report_mart.sql` — запрос пересчёта витрины, DAG читает его из файла.
- `requirements.txt` — пакеты, которые нужно доустановить в образ Airflow.

## Задачи DAG

| Задача | Что делает |
| --- | --- |
| `extract_crm_clients` | Читает `public.clients` из CRM (PostgreSQL) и грузит в `bionicpro.crm_clients` |
| `extract_crm_prostheses` | Читает `public.prostheses` и грузит в `bionicpro.crm_prostheses` |
| `build_report_mart` | Агрегирует `telemetry_raw` по серийнику и суткам, джойнит с CRM, пишет в `bionicpro.user_report_mart` |
| `update_watermark` | Пишет в `bionicpro.etl_watermark` дату, по которую витрина посчитана целиком |

Порядок: `[extract_crm_clients, extract_crm_prostheses] >> build_report_mart >> update_watermark`.

Обе таблицы-реплики CRM — `ReplacingMergeTree(updated_at)`, поэтому перелив целиком безопасен:
старые версии строк схлопываются при слиянии. Витрина пересчитывается за последние 7 суток
(переменная окружения `MART_WINDOW_DAYS`), чтобы подхватить опоздавшую телеметрию; перед вставкой
строки за это окно удаляются через `ALTER TABLE ... DELETE`, так что повторный запуск не плодит дубли.

Витрина считается по вчерашний день включительно — текущие незакрытые сутки в неё не попадают,
и в `etl_watermark` уходит именно эта дата. Сервис отчётов обрезает по ней запрошенный период.

## Подключения

Postgres берётся из Airflow Connection `crm_postgres`. Если его нет, DAG подключается по переменным
окружения (`CRM_DB_HOST`, `CRM_DB_PORT`, `CRM_DB_NAME`, `CRM_DB_USER`, `CRM_DB_PASSWORD`) —
значения по умолчанию совпадают с настройками compose, так что DAG работает сразу.

Коннект можно завести вручную:

```bash
docker compose exec airflow airflow connections add crm_postgres \
  --conn-type postgres --conn-host crm_db --conn-port 5432 \
  --conn-schema crm --conn-login crm_user --conn-password crm_password
```

ClickHouse — по HTTP (8123) через `clickhouse-connect`, параметры из
`CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.

## Запуск

```bash
docker compose up -d crm_db clickhouse airflow-webserver airflow-scheduler
# UI: http://localhost:8081, DAG включается тумблером
docker compose exec airflow airflow dags trigger crm_to_clickhouse_etl
```

Проверить результат:

```bash
docker compose exec clickhouse clickhouse-client -u analytics --password analytics_password -q \
  "SELECT * FROM bionicpro.user_report_mart FINAL WHERE user_id = 'prothetic1' ORDER BY report_date DESC LIMIT 10"

docker compose exec clickhouse clickhouse-client -u analytics --password analytics_password -q \
  "SELECT * FROM bionicpro.etl_watermark FINAL"
```
