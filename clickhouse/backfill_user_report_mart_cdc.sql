-- Бэкфилл CDC-витрины по всей уже накопленной телеметрии.
--
-- MATERIALIZED VIEW mv_user_report_mart_cdc считает только свежие вставки в
-- telemetry_raw, а справочники CRM приезжают из Kafka уже после старта стенда.
-- Поэтому первый расчёт витрины делаем этим запросом — его запускает
-- debezium/register-connector.sh сразу после того, как снапшот Debezium доехал.
-- Его же можно прогнать руками, если справочники поменялись задним числом:
-- витрина схлопнется по ReplacingMergeTree.

INSERT INTO bionicpro.user_report_mart_cdc
SELECT
    cl.user_id         AS user_id,
    agg.report_date    AS report_date,
    cl.full_name       AS full_name,
    agg.serial_number  AS serial_number,
    pr.model           AS model,
    agg.sessions       AS sessions,
    agg.gestures_total AS gestures_total,
    agg.avg_latency_ms AS avg_latency_ms,
    agg.max_latency_ms AS max_latency_ms,
    agg.p95_latency_ms AS p95_latency_ms,
    agg.avg_battery    AS avg_battery,
    agg.min_battery    AS min_battery,
    agg.errors         AS errors,
    now()              AS updated_at
FROM
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
    FROM bionicpro.telemetry_raw
    GROUP BY serial_number, report_date
) AS agg
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
-- 5. Водяной знак ETL
-- ---------------------------------------------------------------------------
-- Одна строка на витрину: до какой даты данные посчитаны. Используется
-- мониторингом и Airflow, чтобы понимать свежесть отчётов.

CREATE TABLE IF NOT EXISTS bionicpro.etl_watermark
(
    mart            String,
    processed_until Date,
    updated_at      DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mart;

-- Обновляется автоматически: MV ловит вставку в витрину и пишет максимальную
-- дату из вставленного блока. ReplacingMergeTree(updated_at) оставит последнюю
-- по времени запись, поэтому при догрузке старых дней значение может уехать
-- назад — в этом случае достаточно перезапустить пересчёт из п. 6.
DROP VIEW IF EXISTS bionicpro.mv_etl_watermark_cdc;
CREATE MATERIALIZED VIEW bionicpro.mv_etl_watermark_cdc TO bionicpro.etl_watermark AS
SELECT
    'user_report_mart_cdc'                    AS mart,
    -- текущие сутки ещё не закрыты, в отчёт они попадать не должны
    least(max(report_date), today() - 1)      AS processed_until,
    now()                                     AS updated_at
FROM bionicpro.user_report_mart_cdc;

-- 6. Пересчёт водяного знака по всей витрине (первичная инициализация
--    и «ремонт» после бэкфилла старых периодов).
INSERT INTO bionicpro.etl_watermark
SELECT
    'user_report_mart_cdc'               AS mart,
    least(max(report_date), today() - 1) AS processed_until,
    now()                                AS updated_at
FROM bionicpro.user_report_mart_cdc
-- пока витрина пуста, отметку не пишем: иначе в неё попал бы 1970-01-01
HAVING count() > 0;

-- Читать водяной знак так:
--   SELECT processed_until FROM bionicpro.etl_watermark FINAL
--   WHERE mart = 'user_report_mart_cdc';
