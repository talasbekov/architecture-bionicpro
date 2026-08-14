-- Витрина отчётности v2: строится на лету из telemetry_raw + справочники из CDC.
-- Отличие от user_report_mart (v1, наполняется батчем из Airflow) в том, что
-- пересчёт запускается самой вставкой телеметрии, а справочники клиентов
-- и протезов приезжают из CRM почти в реальном времени через Debezium.

CREATE DATABASE IF NOT EXISTS bionicpro;

-- ---------------------------------------------------------------------------
-- 1. Целевая витрина
-- ---------------------------------------------------------------------------
-- Набор колонок совпадает с user_report_mart, чтобы reports-api мог
-- переключаться между витринами одной переменной окружения.

CREATE TABLE IF NOT EXISTS bionicpro.user_report_mart_cdc
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

-- ---------------------------------------------------------------------------
-- 2. Обходной VIEW над telemetry_raw
-- ---------------------------------------------------------------------------
-- В ClickHouse MATERIALIZED VIEW видит не всю таблицу-источник, а только
-- свежевставленный блок строк. Если считать агрегаты прямо по блоку, в витрину
-- попадут частичные суммы за день (а ReplacingMergeTree их не складывает,
-- а заменяет). Поэтому в MV: слева — блок (нужен только для списка изменённых
-- пар «серийник + дата»), справа — полный пересчёт этих дней по обычному VIEW.
-- Подстановка блока работает только для таблицы в основном FROM, обращение
-- к telemetry_raw_src читает реальные данные.

CREATE VIEW IF NOT EXISTS bionicpro.telemetry_raw_src AS
SELECT * FROM bionicpro.telemetry_raw;

-- ---------------------------------------------------------------------------
-- 3. MV: пересчёт витрины при вставке телеметрии
-- ---------------------------------------------------------------------------
-- Справочники читаем с FINAL и фильтром is_deleted = 0: таблицы маленькие
-- (тысячи строк), FINAL здесь дешевле и нагляднее, чем поднимать словари.
-- Альтернатива для больших объёмов — CREATE DICTIONARY над crm_*_cdc
-- и dictGet(), тогда JOIN заменяется на dictGetString('dict_prostheses', ...).
--
-- sessions считаем как число часов, в которые была активность:
-- отдельного session_id в телеметрии нет.

DROP VIEW IF EXISTS bionicpro.mv_user_report_mart_cdc;
CREATE MATERIALIZED VIEW bionicpro.mv_user_report_mart_cdc TO bionicpro.user_report_mart_cdc AS
SELECT DISTINCT
    cl.user_id           AS user_id,
    agg.report_date      AS report_date,
    cl.full_name         AS full_name,
    agg.serial_number    AS serial_number,
    pr.model             AS model,
    agg.sessions         AS sessions,
    agg.gestures_total   AS gestures_total,
    agg.avg_latency_ms   AS avg_latency_ms,
    agg.max_latency_ms   AS max_latency_ms,
    agg.p95_latency_ms   AS p95_latency_ms,
    agg.avg_battery      AS avg_battery,
    agg.min_battery      AS min_battery,
    agg.errors           AS errors,
    now()                AS updated_at
FROM bionicpro.telemetry_raw AS blk
INNER JOIN
(
    SELECT
        prosthesis_serial                          AS serial_number,
        toDate(event_time)                         AS report_date,
        uniqExact(toStartOfHour(event_time))       AS sessions,
        count()                                    AS gestures_total,
        toFloat32(avg(latency_ms))                 AS avg_latency_ms,
        toUInt16(max(latency_ms))                  AS max_latency_ms,
        toFloat32(quantileExact(0.95)(latency_ms)) AS p95_latency_ms,
        toFloat32(avg(battery_level))              AS avg_battery,
        toUInt8(min(battery_level))                AS min_battery,
        countIf(error_code != '')                  AS errors
    FROM bionicpro.telemetry_raw_src
    GROUP BY serial_number, report_date
) AS agg
    ON agg.serial_number = blk.prosthesis_serial
   AND agg.report_date = toDate(blk.event_time)
INNER JOIN
(
    SELECT client_id, serial_number, model
    FROM bionicpro.crm_prostheses_cdc FINAL
    WHERE is_deleted = 0
) AS pr ON pr.serial_number = agg.serial_number
INNER JOIN
(
    SELECT id, user_id, full_name
    FROM bionicpro.crm_clients_cdc FINAL
    WHERE is_deleted = 0
) AS cl ON cl.id = pr.client_id;

-- ---------------------------------------------------------------------------
-- 4. Первичный бэкфилл
-- ---------------------------------------------------------------------------
-- Здесь его делать нечего: на момент инициализации ClickHouse справочники CRM
-- ещё не приехали из Kafka, join дал бы ноль строк. Витрину наполняет
-- clickhouse/backfill_user_report_mart_cdc.sql — его запускает
-- debezium/register-connector.sh после регистрации коннектора.
