import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query

from . import olap
from .config import settings
from .security import CurrentUser, get_current_user
from .storage import ReportStorage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("reports-api")

app = FastAPI(title="reports-api")
storage = ReportStorage()


@app.on_event("startup")
def startup() -> None:
    storage.ensure_bucket()


@app.get("/health")
def health():
    return {"status": "ok", "mart": settings.mart_table}


def parse_period(date_from: Optional[str], date_to: Optional[str]) -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()
    try:
        start = date.fromisoformat(date_from) if date_from else today - timedelta(
            days=settings.default_period_days
        )
        end = date.fromisoformat(date_to) if date_to else today
    except ValueError:
        raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD")
    if start > end:
        raise HTTPException(status_code=400, detail="Начало периода позже конца")
    return start, end


def build_report(user_id: str, start: date, end: date, watermark: date) -> dict:
    rows = olap.fetch_report_rows(user_id, start, end)

    devices: dict[str, dict] = {}
    for row in rows:
        serial = row["serial_number"]
        device = devices.setdefault(
            serial,
            {
                "serialNumber": serial,
                "model": row["model"],
                "sessions": 0,
                "gestures": 0,
                "errors": 0,
                "maxLatencyMs": 0,
                "_latencySum": 0.0,
                "_batterySum": 0.0,
                "_days": 0,
            },
        )
        device["sessions"] += int(row["sessions"] or 0)
        device["gestures"] += int(row["gestures_total"] or 0)
        device["errors"] += int(row["errors"] or 0)
        device["maxLatencyMs"] = max(device["maxLatencyMs"], int(row["max_latency_ms"] or 0))
        device["_latencySum"] += float(row["avg_latency_ms"] or 0)
        device["_batterySum"] += float(row["avg_battery"] or 0)
        device["_days"] += 1

    for device in devices.values():
        days = device.pop("_days") or 1
        device["avgLatencyMs"] = round(device.pop("_latencySum") / days, 1)
        device["avgBatteryPercent"] = round(device.pop("_batterySum") / days, 1)

    full_name = rows[0]["full_name"] if rows else None

    return {
        "userId": user_id,
        "fullName": full_name,
        "period": {"from": start.isoformat(), "to": end.isoformat()},
        "dataProcessedUntil": watermark.isoformat(),
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "devices": list(devices.values()),
        "daily": [
            {
                "date": row["report_date"].isoformat()
                if hasattr(row["report_date"], "isoformat")
                else str(row["report_date"]),
                "serialNumber": row["serial_number"],
                "sessions": int(row["sessions"] or 0),
                "gestures": int(row["gestures_total"] or 0),
                "avgLatencyMs": round(float(row["avg_latency_ms"] or 0), 1),
                "p95LatencyMs": round(float(row["p95_latency_ms"] or 0), 1),
                "maxLatencyMs": int(row["max_latency_ms"] or 0),
                "avgBatteryPercent": round(float(row["avg_battery"] or 0), 1),
                "minBatteryPercent": int(row["min_battery"] or 0),
                "errors": int(row["errors"] or 0),
            }
            for row in rows
        ],
    }


@app.get("/reports")
def get_report(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    user: Optional[str] = Query(None, description="Только собственный идентификатор"),
    current: CurrentUser = Depends(get_current_user),
):
    # Отчёт всегда строится по пользователю из токена. Чужой идентификатор — отказ.
    if user and user != current.user_id:
        raise HTTPException(status_code=403, detail="Доступен только собственный отчёт")

    user_id = current.user_id
    start, end = parse_period(date_from, date_to)

    watermark = olap.processed_until()
    if watermark is None:
        raise HTTPException(
            status_code=503,
            detail="Витрина ещё не наполнена, отчёт будет доступен после первого запуска ETL",
        )
    if start > watermark:
        raise HTTPException(
            status_code=409,
            detail=f"Данные обработаны только по {watermark.isoformat()}, выберите более ранний период",
        )
    # период обрезаем по границе обработанных данных
    end = min(end, watermark)

    key = storage.build_key(user_id, start.isoformat(), end.isoformat(), watermark.isoformat())

    if storage.exists(key):
        log.info("Отчёт %s уже есть в хранилище, OLAP не трогаем", key)
        return {
            "userId": user_id,
            "period": {"from": start.isoformat(), "to": end.isoformat()},
            "dataProcessedUntil": watermark.isoformat(),
            "cached": True,
            "reportUrl": storage.cdn_url(key),
        }

    report = build_report(user_id, start, end, watermark)
    storage.put(key, report)
    log.info("Отчёт %s построен из витрины %s и сохранён в S3", key, settings.mart_table)

    return {
        "userId": user_id,
        "period": {"from": start.isoformat(), "to": end.isoformat()},
        "dataProcessedUntil": watermark.isoformat(),
        "cached": False,
        "reportUrl": storage.cdn_url(key),
    }


@app.get("/reports/data")
def get_report_data(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    current: CurrentUser = Depends(get_current_user),
):
    """Тот же отчёт, но телом ответа — на случай, если CDN недоступен."""
    start, end = parse_period(date_from, date_to)
    watermark = olap.processed_until()
    if watermark is None:
        raise HTTPException(status_code=503, detail="Витрина ещё не наполнена")
    if start > watermark:
        raise HTTPException(status_code=409, detail="Запрошен период, который ещё не обработан")
    end = min(end, watermark)

    key = storage.build_key(
        current.user_id, start.isoformat(), end.isoformat(), watermark.isoformat()
    )
    cached = storage.get(key)
    if cached is not None:
        return cached

    report = build_report(current.user_id, start, end, watermark)
    storage.put(key, report)
    return report
