-- Пересчёт витрины user_report_mart за окно [{date_from}; {date_to}].
-- Телеметрия агрегируется по паре серийник + сутки, затем обогащается данными CRM.
INSERT INTO bionicpro.user_report_mart
(
    user_id, report_date, full_name, serial_number, model,
    sessions, gestures_total, avg_latency_ms, max_latency_ms, p95_latency_ms,
    avg_battery, min_battery, errors, updated_at
)
SELECT
    c.user_id                       AS user_id,
    t.report_date                   AS report_date,
    c.full_name                     AS full_name,
    t.serial_number                 AS serial_number,
    p.model                         AS model,
    t.sessions                      AS sessions,
    t.gestures_total                AS gestures_total,
    t.avg_latency_ms                AS avg_latency_ms,
    t.max_latency_ms                AS max_latency_ms,
    t.p95_latency_ms                AS p95_latency_ms,
    t.avg_battery                   AS avg_battery,
    t.min_battery                   AS min_battery,
    t.errors                        AS errors,
    now()                           AS updated_at
FROM
(
    SELECT
        prosthesis_serial                            AS serial_number,
        toDate(event_time)                           AS report_date,
        -- сессией считаем час активности: подряд идущие жесты внутри часа — одна сессия
        toUInt64(uniq(toStartOfHour(event_time)))     AS sessions,
        toUInt64(count())                            AS gestures_total,
        toFloat32(avg(latency_ms))                   AS avg_latency_ms,
        toUInt16(max(latency_ms))                    AS max_latency_ms,
        toFloat32(quantile(0.95)(latency_ms))        AS p95_latency_ms,
        toFloat32(avg(battery_level))                AS avg_battery,
        toUInt8(min(battery_level))                  AS min_battery,
        toUInt64(countIf(error_code != ''))          AS errors
    FROM bionicpro.telemetry_raw
    WHERE event_time >= toDateTime('{date_from} 00:00:00')
      AND event_time <  toDateTime('{date_to} 00:00:00') + INTERVAL 1 DAY
    GROUP BY serial_number, report_date
) AS t
INNER JOIN
(
    -- FINAL, чтобы взять актуальную версию строки из ReplacingMergeTree
    SELECT id, client_id, serial_number, model FROM bionicpro.crm_prostheses FINAL
) AS p ON p.serial_number = t.serial_number
INNER JOIN
(
    SELECT client_id, user_id, full_name FROM bionicpro.crm_clients FINAL
) AS c ON c.client_id = p.client_id;
