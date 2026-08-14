#!/usr/bin/env bash
# Регистрация Debezium-коннектора к CRM PostgreSQL в Kafka Connect.
# Идемпотентно: если коннектора нет — создаём (POST /connectors),
# если уже есть — обновляем конфиг (PUT /connectors/<name>/config).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="${CONFIG_FILE:-$(dirname "$0")/register-crm-connector.json}"
WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-60}"
WAIT_DELAY="${WAIT_DELAY:-5}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Не найден файл конфигурации: $CONFIG_FILE" >&2
    exit 1
fi

# Из файла берём и имя, и вложенный объект config (для PUT нужен только он).
# Работаем через jq, если он есть, иначе через python3.
if command -v jq >/dev/null 2>&1; then
    json_get() { jq -r "$1" "$CONFIG_FILE"; }
elif command -v python3 >/dev/null 2>&1; then
    json_get() {
        case "$1" in
            .name)   python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$CONFIG_FILE" ;;
            *)       python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$CONFIG_FILE" ;;
        esac
    }
else
    echo "Нужен jq или python3 для разбора $CONFIG_FILE" >&2
    exit 1
fi

CONNECTOR_NAME="$(json_get .name)"

echo "Ждём готовности Kafka Connect на $CONNECT_URL ..."
attempt=1
until curl -sf "$CONNECT_URL/connectors" >/dev/null 2>&1; do
    if [ "$attempt" -ge "$WAIT_ATTEMPTS" ]; then
        echo "Kafka Connect не поднялся за $((WAIT_ATTEMPTS * WAIT_DELAY)) секунд" >&2
        exit 1
    fi
    echo "  попытка $attempt/$WAIT_ATTEMPTS ..."
    attempt=$((attempt + 1))
    sleep "$WAIT_DELAY"
done
echo "Kafka Connect готов."

if curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" >/dev/null 2>&1; then
    echo "Коннектор '$CONNECTOR_NAME' уже существует — обновляем конфиг."
    # PUT принимает только тело config, без обёртки name/config
    json_get .config \
        | curl -sS -X PUT \
            -H "Content-Type: application/json" \
            --data @- \
            "$CONNECT_URL/connectors/$CONNECTOR_NAME/config"
else
    echo "Создаём коннектор '$CONNECTOR_NAME'."
    curl -sS -X POST \
        -H "Content-Type: application/json" \
        --data @"$CONFIG_FILE" \
        "$CONNECT_URL/connectors"
fi

echo
echo "Ждём готовности коннектора ..."
attempt=1
until curl -sS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null | grep -q '"state":"RUNNING"'; do
    if [ "$attempt" -ge "$WAIT_ATTEMPTS" ]; then
        echo "Коннектор не перешёл в RUNNING" >&2
        curl -sS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep "$WAIT_DELAY"
done

echo "Статус коннектора:"
curl -sS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
echo

# Снапшот справочников доезжает в ClickHouse через Kafka с задержкой,
# и только после этого имеет смысл считать CDC-витрину.
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CH="docker compose -f $REPO_DIR/docker-compose.yaml exec -T clickhouse clickhouse-client -u analytics --password analytics_password -d bionicpro"

echo "Ждём снапшот справочников CRM в ClickHouse ..."
attempt=1
until [ "$($CH -q 'SELECT count() FROM crm_clients_cdc' 2>/dev/null || echo 0)" != "0" ] \
   && [ "$($CH -q 'SELECT count() FROM crm_prostheses_cdc' 2>/dev/null || echo 0)" != "0" ]; do
    if [ "$attempt" -ge "$WAIT_ATTEMPTS" ]; then
        echo "Данные CDC не появились в ClickHouse" >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep "$WAIT_DELAY"
done

echo "Считаем витрину user_report_mart_cdc по накопленной телеметрии ..."
$CH --multiquery < "$REPO_DIR/clickhouse/backfill_user_report_mart_cdc.sql"
echo "Готово, строк в витрине: $($CH -q 'SELECT count() FROM user_report_mart_cdc FINAL')"
