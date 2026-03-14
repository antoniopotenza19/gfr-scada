import logging
from datetime import UTC, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import AlarmEvent as AlarmEventModel
from ..schemas import AlarmCreateIn, AlarmEvent, DashboardMonthlyOverview, PlantSummary, PlantTimeseries
from ..services.energysaving_runtime import (
    fetch_dashboard_monthly_overview,
    fetch_dashboard_summary,
    fetch_dashboard_timeseries,
    list_enabled_sale_codes,
)

router = APIRouter(prefix="/api/plants")
LOGGER = logging.getLogger(__name__)


def empty_summary_payload(target: str) -> dict:
    return {
        "plant": target,
        "last_update": None,
        "signals": {},
        "compressors": [],
        "dryers": [],
        "active_alarms": [],
    }


def _parse_alarm_iso_timestamp(value: Optional[str], field_name: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid {field_name} timestamp") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _serialize_alarm_timestamp(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


@router.get("/", response_model=List[str])
def list_plants():
    plants = list_enabled_sale_codes()
    LOGGER.debug("dashboard_db_plants count=%s", len(plants))
    return plants


@router.get("/{plant}/summary", response_model=PlantSummary)
def plant_summary(
    plant: str,
    room: Optional[str] = Query(None),
):
    target = room or plant
    payload = fetch_dashboard_summary(target)
    if payload is None:
        raise HTTPException(status_code=404, detail="plant not found")

    LOGGER.debug(
        "dashboard_refresh_summary target=%s room=%s signals=%s compressors=%s",
        plant,
        room,
        len(payload["signals"]),
        len(payload["compressors"]),
    )
    if not payload["signals"]:
        return empty_summary_payload(payload["plant"])
    return payload


@router.get("/{plant}/timeseries", response_model=PlantTimeseries)
def plant_timeseries(
    plant: str,
    signal: str = Query(...),
    room: Optional[str] = Query(None),
    minutes: int = Query(60, ge=1),
    max_points: int = Query(5000, ge=1, le=20000),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    bucket: Optional[str] = Query(None),
    agg: str = Query("avg", pattern="^(avg|sum|min|max)$"),
):
    target = room or plant
    try:
        points = fetch_dashboard_timeseries(
            identifier=target,
            signal=signal,
            from_ts=from_ts,
            to_ts=to_ts,
            minutes=minutes,
            max_points=max_points,
            bucket=bucket,
            agg=agg,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if points is None:
        raise HTTPException(status_code=404, detail="plant or signal not found")

    LOGGER.debug(
        "dashboard_refresh_timeseries target=%s room=%s signal=%s points=%s",
        plant,
        room,
        signal,
        len(points),
    )
    return {"plant": target, "signal": signal, "points": points}


@router.get("/{plant}/monthly-overview", response_model=DashboardMonthlyOverview)
def plant_monthly_overview(
    plant: str,
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
):
    payload = fetch_dashboard_monthly_overview(plant, from_ts=from_ts, to_ts=to_ts)
    if payload is None:
        raise HTTPException(status_code=404, detail="plant not found")

    LOGGER.debug(
        "dashboard_monthly_overview target=%s volume_points=%s energy_points=%s",
        plant,
        len(payload["volume_points"]),
        len(payload["energy_points"]),
    )
    return payload


@router.get("/{plant}/alarms", response_model=List[AlarmEvent])
def plant_alarms(
    plant: str,
    room: Optional[str] = Query(None),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(1000, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    from_value = _parse_alarm_iso_timestamp(from_ts, "from")
    to_value = _parse_alarm_iso_timestamp(to_ts, "to")
    if from_value and to_value and from_value > to_value:
        raise HTTPException(status_code=400, detail="'from' must be before 'to'")

    statement = select(AlarmEventModel)
    if room:
        statement = statement.where(AlarmEventModel.room == room)
    else:
        statement = statement.where(or_(AlarmEventModel.room == plant, AlarmEventModel.plant == plant))

    if from_value:
        statement = statement.where(AlarmEventModel.ts >= from_value)
    if to_value:
        statement = statement.where(AlarmEventModel.ts <= to_value)

    rows = (
        db.execute(
            statement.order_by(AlarmEventModel.ts.desc(), AlarmEventModel.id.desc()).limit(limit)
        )
        .scalars()
        .all()
    )

    alarms = [
        AlarmEvent(
            id=str(row.id),
            code=row.signal or f"ALARM-{row.id}",
            severity=row.severity or "info",
            message=row.message or "",
            ts=_serialize_alarm_timestamp(row.ts) or "",
            room=row.room,
            plant=row.plant,
            active=row.active,
            ack_user=row.ack_user,
            ack_time=_serialize_alarm_timestamp(row.ack_time),
        )
        for row in rows
    ]
    LOGGER.debug("dashboard_refresh_alarms target=%s room=%s count=%s", plant, room, len(alarms))
    return alarms


@router.post("/{plant}/alarms")
def create_alarm(
    plant: str,
    payload: AlarmCreateIn,
):
    LOGGER.warning("dashboard_alarm_create_not_supported plant=%s signal=%s", plant, payload.signal)
    raise HTTPException(status_code=501, detail="Alarm creation is not supported on gfr_energysaving")
