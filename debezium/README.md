# CDC: CRM PostgreSQL -> Debezium -> Kafka -> ClickHouse

Витрина отчётности v1 (`user_report_mart`) наполняется батчем из Airflow: справочники
клиентов и протезов выгружаются из CRM раз в сутки. Витрина v2 (`user_report_mart_cdc`)
получает изменения CRM почти сразу — через логическую репликацию Postgres.

## Схема потока

```
crm_db (PostgreSQL, wal_level=logical)
   |  логический слот crm_slot, публикация crm_pub, плагин pgoutput
   v
kafka-connect:8083  (образ debezium/connect, коннектор crm-connector)
   |  топики crm.public.clients, crm.public.prostheses
   v
kafka:9092
   |  Kafka Engine: kafka_crm_clients, kafka_crm_prostheses
   v
clickhouse: MV mv_crm_clients / mv_crm_prostheses
   v
crm_clients_cdc, crm_prostheses_cdc   (ReplacingMergeTree(updated_at))
   |
   +--> join к telemetry_raw --> user_report_mart_cdc
```

## Что где лежит

| Файл | Назначение |
| --- | --- |
| `debezium/register-crm-connector.json` | конфиг коннектора `crm-connector` |
| `debezium/register-connector.sh` | идемпотентная регистрация коннектора в Kafka Connect |
| `clickhouse/init/10_cdc_kafka.sql` | Kafka Engine таблицы, целевые CDC-таблицы и MV |
| `clickhouse/init/11_cdc_mart.sql` | витрина `user_report_mart_cdc`, бэкфилл, `etl_watermark` |

## Требования к Postgres

CRM должна быть запущена с логическим WAL — в `docker-compose.yaml` у сервиса `crm_db`
прописано `command: postgres -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4`.
Без этого коннектор не сможет создать слот `crm_slot`.

Таблицам выставлен `REPLICA IDENTITY FULL` (см. `crm/init/01_schema.sql`) — иначе при
UPDATE/DELETE в WAL попадает только первичный ключ и Debezium не отдаст остальные поля.

Публикация `crm_pub` создаётся коннектором автоматически (`publication.autocreate.mode:
filtered`) только на таблицы из `table.include.list`.

## Как форматируются сообщения

- `ExtractNewRecordState` (unwrap) убирает конверт `before/after/op` — в топик уходит
  плоский JSON со значениями строки.
- `delete.handling.mode: rewrite` — DELETE приезжает не как tombstone, а как обычная
  строка с дополнительным полем `__deleted: "true"`. Tombstone-сообщения отключены
  (`tombstones.on.delete: false`), потому что Kafka Engine споткнулся бы на пустом теле.
- `schemas.enable: false` — значения без схемы, ClickHouse читает их `JSONEachRow`.
- `time.precision.mode: adaptive` (выбранный рабочий вариант декодирования дат):
  - `TIMESTAMPTZ` (`created_at`, `updated_at`) -> строка ISO-8601 в UTC,
    в ClickHouse читается как `String` и разбирается `parseDateTimeBestEffortOrZero()`;
  - `DATE` (`activated_at`) -> число дней от 1970-01-01, читается как `Int32`
    и переводится `toDate()`.

  Так не нужны кастомные конвертеры в Kafka Connect, вся конвертация — на стороне
  ClickHouse в материализованных представлениях.

Пример сообщения из `crm.public.clients`:

```json
{"id":1,"user_id":"prothetic1","full_name":"Иван Петров","email":"prothetic1@example.com",
 "country":"Russia","created_at":"2024-01-10T09:00:00Z","updated_at":"2024-01-10T09:00:00Z",
 "__deleted":"false","__op":"r","__source_ts_ms":1707000000000}
```

## Запуск

```bash
docker compose up -d crm_db kafka kafka-connect clickhouse

# регистрация коннектора (идемпотентно: есть -> PUT /config, нет -> POST /connectors)
CONNECT_URL=http://localhost:8083 ./debezium/register-connector.sh
```

Скрипт сам ждёт готовности Kafka Connect (до 5 минут) и в конце печатает статус.

Проверить, что коннектор жив:

```bash
curl -s localhost:8083/connectors/crm-connector/status | jq
# state должен быть RUNNING и у коннектора, и у таска
```

## Проверка потока

1. Топики созданы и в них есть снапшот:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
# ожидаем crm.public.clients и crm.public.prostheses

docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic crm.public.clients \
  --from-beginning --max-messages 5
```

2. Данные доехали до ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client \
  -u analytics --password analytics_password -d bionicpro \
  -q "SELECT id, user_id, full_name, is_deleted FROM crm_clients_cdc FINAL ORDER BY id"
```

3. Меняем строку в CRM и смотрим, как изменение приезжает:

```bash
docker compose exec crm_db psql -U crm_user -d crm -c \
  "UPDATE clients SET full_name = 'Иван Петров-Водкин', updated_at = now() WHERE user_id = 'prothetic1';"

# через 1-2 секунды
docker compose exec clickhouse clickhouse-client \
  -u analytics --password analytics_password -d bionicpro \
  -q "SELECT full_name, updated_at FROM crm_clients_cdc FINAL WHERE user_id = 'prothetic1'"
```

Удаление проверяется так же: после `DELETE FROM prostheses WHERE serial_number = '...'`
строка остаётся в `crm_prostheses_cdc` с `is_deleted = 1` (мягкое удаление, чтобы
исторические отчёты не разъехались). Все выборки для витрины фильтруют `is_deleted = 0`.

4. Витрина пересчитывается при вставке телеметрии:

```bash
docker compose exec clickhouse clickhouse-client \
  -u analytics --password analytics_password -d bionicpro -q "
    SELECT user_id, report_date, serial_number, gestures_total, p95_latency_ms
    FROM user_report_mart_cdc FINAL
    ORDER BY report_date DESC LIMIT 10"

docker compose exec clickhouse clickhouse-client \
  -u analytics --password analytics_password -d bionicpro -q "
    SELECT * FROM etl_watermark FINAL WHERE mart = 'user_report_mart_cdc'"
```

## Тонкости, о которые легко споткнуться

- MV в ClickHouse — это триггер на вставку, он не видит уже лежащие данные. Поэтому
  в `11_cdc_mart.sql` есть отдельный `INSERT ... SELECT` для первичного бэкфилла
  телеметрии. Его же надо прогнать, если справочники CRM поменялись задним числом.
- `ReplacingMergeTree` схлопывает версии только при слиянии кусков, поэтому во всех
  запросах к `crm_*_cdc` и к витрине используется `FINAL`.
- При DELETE Debezium в режиме rewrite отдаёт старые значения полей, включая старый
  `updated_at`. В MV версией удаления ставится `now()`, иначе удаление могло бы
  проиграть живой версии строки при схлопывании.
- Если нужно перечитать CRM с нуля: удалить коннектор
  (`curl -X DELETE localhost:8083/connectors/crm-connector`), дропнуть слот в Postgres
  (`SELECT pg_drop_replication_slot('crm_slot');`), очистить `crm_*_cdc` и
  зарегистрировать коннектор заново — снапшот `initial` перельёт таблицы целиком.
- Kafka Engine читает топик один раз на группу `clickhouse_crm`. Пересоздавать
  Kafka-таблицу без надобности не стоит: офсеты хранятся в Kafka по имени группы.

## Переключение сервиса отчётов на витрину v2

`reports-api` берёт имя таблицы из переменной окружения `REPORT_MART_TABLE`.
По умолчанию это `user_report_mart` (v1, батч из Airflow). Для перехода на CDC-витрину
в `docker-compose.yaml` у сервиса `reports-api`:

```yaml
    environment:
      REPORT_MART_TABLE: user_report_mart_cdc
```

и перезапустить сервис: `docker compose up -d reports-api`. Схема колонок у обеих
витрин одинаковая, поэтому код запросов не меняется. Откат — вернуть прежнее значение
переменной.
