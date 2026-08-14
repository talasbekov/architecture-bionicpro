-- Приём CDC-потока из CRM PostgreSQL: Debezium -> Kafka -> ClickHouse.
-- Цепочка на каждую таблицу: Kafka Engine (сырой JSON) -> MATERIALIZED VIEW -> ReplacingMergeTree.
--
-- Формат сообщений задан коннектором debezium/register-crm-connector.json:
--   * ExtractNewRecordState (unwrap) — плоский JSON без конверта before/after;
--   * delete.handling.mode = rewrite — DELETE приезжает как обычная строка с __deleted = "true";
--   * schemas.enable = false — значения приходят «как есть»;
--   * time.precision.mode = adaptive:
--       - TIMESTAMPTZ (created_at/updated_at) -> строка ISO-8601 в UTC, читаем как String
--         и разбираем parseDateTimeBestEffortOrZero();
--       - DATE (activated_at) -> число дней от 1970-01-01, читаем как Int32 и переводим toDate().
--     Такой вариант не требует кастомных конвертеров в Kafka Connect.

CREATE DATABASE IF NOT EXISTS bionicpro;

-- ---------------------------------------------------------------------------
-- 1. Kafka Engine — очередь, читается ровно один раз на каждый MV
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bionicpro.kafka_crm_clients
(
    id          Int64,
    user_id     String,
    full_name   String,
    email       String,
    country     String,
    created_at  String,
    updated_at  String,
    `__deleted` String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.clients',
    kafka_group_name = 'clickhouse_crm',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    -- Debezium добавляет служебные поля (__op, __source_ts_ms) — их игнорируем.
    input_format_skip_unknown_fields = 1,
    -- NULL из Postgres (email, country) кладём как значение по умолчанию.
    input_format_null_as_default = 1,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bionicpro.kafka_crm_prostheses
(
    id            Int64,
    client_id     Int64,
    serial_number String,
    model         String,
    status        String,
    activated_at  Int32,
    updated_at    String,
    `__deleted`   String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.prostheses',
    kafka_group_name = 'clickhouse_crm',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default = 1,
    kafka_skip_broken_messages = 10;

-- ---------------------------------------------------------------------------
-- 2. Целевые таблицы — актуальный слепок справочников CRM
-- ---------------------------------------------------------------------------
-- ReplacingMergeTree(updated_at): при слиянии по ключу остаётся строка
-- с максимальным updated_at. Читать нужно с FINAL (или argMax), иначе до мержа
-- видны обе версии строки.

CREATE TABLE IF NOT EXISTS bionicpro.crm_clients_cdc
(
    id         Int64,
    user_id    String,
    full_name  String,
    email      String,
    country    String,
    created_at DateTime,
    updated_at DateTime,
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE TABLE IF NOT EXISTS bionicpro.crm_prostheses_cdc
(
    id            Int64,
    client_id     Int64,
    serial_number String,
    model         String,
    status        String,
    activated_at  Date,
    updated_at    DateTime,
    is_deleted    UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- ---------------------------------------------------------------------------
-- 3. Материализованные представления — перекладка из Kafka в целевые таблицы
-- ---------------------------------------------------------------------------
-- Для DELETE Debezium в режиме rewrite отдаёт старые значения полей, включая
-- старый updated_at. Если оставить его как есть, ReplacingMergeTree может
-- предпочесть предыдущую (живую) версию строки и удаление «потеряется».
-- Поэтому версией удаления делаем now().

DROP VIEW IF EXISTS bionicpro.mv_crm_clients;
CREATE MATERIALIZED VIEW bionicpro.mv_crm_clients TO bionicpro.crm_clients_cdc AS
SELECT
    id,
    user_id,
    full_name,
    email,
    country,
    parseDateTimeBestEffortOrZero(created_at) AS created_at,
    if(`__deleted` = 'true', now(), parseDateTimeBestEffortOrZero(updated_at)) AS updated_at,
    toUInt8(`__deleted` = 'true') AS is_deleted
FROM bionicpro.kafka_crm_clients;

DROP VIEW IF EXISTS bionicpro.mv_crm_prostheses;
CREATE MATERIALIZED VIEW bionicpro.mv_crm_prostheses TO bionicpro.crm_prostheses_cdc AS
SELECT
    id,
    client_id,
    serial_number,
    model,
    status,
    -- activated_at приезжает числом дней от эпохи (time.precision.mode = adaptive)
    toDate(activated_at) AS activated_at,
    if(`__deleted` = 'true', now(), parseDateTimeBestEffortOrZero(updated_at)) AS updated_at,
    toUInt8(`__deleted` = 'true') AS is_deleted
FROM bionicpro.kafka_crm_prostheses;
