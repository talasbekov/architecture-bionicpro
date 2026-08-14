-- Демо-телеметрия за последние 60 дней по всем серийникам из CRM.
-- Реальные устройства пишут в telemetry_raw потоком, здесь наполняем таблицу, чтобы витрине было что считать.
--
-- Раскладка номера строки: 10 серийников x 60 дней x 6 сессий x 12 событий = 43 200 строк
-- (по 4 320 на серийник). События внутри одной сессии укладываются в один час —
-- витрина считает сессии как уникальные часы активности.

INSERT INTO bionicpro.telemetry_raw
    (prosthesis_serial, event_time, gesture, latency_ms, battery_level, signal_quality, error_code)
SELECT
    prosthesis_serial,
    event_time,
    gesture,
    latency_ms,
    battery_level,
    signal_quality,
    error_code
FROM
(
    WITH
        ['BP-2024-0001', 'BP-2024-0002', 'BP-2024-0003', 'BP-2024-0004', 'BP-2024-0005',
         'BP-2024-0006', 'BP-2024-0007', 'BP-2024-0008', 'BP-2024-0009', 'BP-2024-0010'] AS serials,
        ['grip', 'pinch', 'point', 'open', 'rotate', 'hold', 'release'] AS gestures,
        ['E01', 'E02', 'E07', 'E13'] AS errors,
        number % 12            AS event_idx,
        intDiv(number, 12) % 6 AS session_idx,
        intDiv(number, 72) % 60 AS day_idx,
        intDiv(number, 4320) % 10 AS serial_idx
    SELECT
        serials[serial_idx + 1] AS prosthesis_serial,
        toStartOfDay(now())
            - toIntervalDay(day_idx)
            + toIntervalHour(9 + session_idx)
            + toIntervalMinute(event_idx * 4 + rand(number) % 4)
            + toIntervalSecond(rand(number + 1) % 60) AS event_time,
        gestures[(rand(number + 2) % length(gestures)) + 1] AS gesture,
        toUInt16(40 + rand(number + 3) % 200) AS latency_ms,
        -- заряд падает от сессии к сессии в течение дня
        toUInt8(greatest(5, 100 - session_idx * 13 - event_idx)) AS battery_level,
        toFloat32(0.6 + (rand(number + 4) % 400) / 1000) AS signal_quality,
        -- примерно 2% событий с ошибкой
        if(rand(number + 5) % 50 = 0, errors[(rand(number + 6) % length(errors)) + 1], '') AS error_code
    FROM numbers(43200)
);
