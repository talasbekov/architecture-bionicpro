-- Аналитическое хранилище BionicPRO.
-- Файл монтируется в /docker-entrypoint-initdb.d контейнера clickhouse.

CREATE DATABASE IF NOT EXISTS bionicpro;

-- Сырая телеметрия с протезов. Пишется потоком, поэтому обычный MergeTree.
CREATE TABLE IF NOT EXISTS bionicpro.telemetry_raw
(
    prosthesis_serial String,
    event_time        DateTime,
    gesture           String,
    latency_ms        UInt16,
    battery_level     UInt8,
    signal_quality    Float32,
    error_code        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (prosthesis_serial, event_time);

-- Реплика клиентов из CRM. ReplacingMergeTree по updated_at:
-- прилетевшее позже обновление вытеснит старую версию строки.
CREATE TABLE IF NOT EXISTS bionicpro.crm_clients
(
    user_id    String,
    client_id  UInt64,
    full_name  String,
    email      String,
    country    String,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY client_id;

-- Реплика протезов из CRM.
CREATE TABLE IF NOT EXISTS bionicpro.crm_prostheses
(
    id            UInt64,
    client_id     UInt64,
    serial_number String,
    model         String,
    status        String,
    updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- Витрина отчётности. Сортировка начинается с user_id, потому что
-- сервис отчётов всегда выбирает данные по одному пользователю за период.
CREATE TABLE IF NOT EXISTS bionicpro.user_report_mart
(
    user_id        String,
    report_date    Date,
    full_name      String,
    serial_number  String,
    model          String,
    sessions       UInt64,
    gestures_total UInt64,
    avg_latency_ms Float32,
    max_latency_ms UInt16,
    p95_latency_ms Float32,
    avg_battery    Float32,
    min_battery    UInt8,
    errors         UInt64,
    updated_at     DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (user_id, report_date, serial_number);

-- Отметка о том, по какую дату витрина посчитана полностью.
-- Сервис отчётов обрезает по ней запрошенный период, чтобы не отдавать неполные сутки.
CREATE TABLE IF NOT EXISTS bionicpro.etl_watermark
(
    mart            String,
    processed_until Date,
    updated_at      DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mart;
