import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..schemas import AlarmCreateIn, AlarmEvent, PlantSummary, PlantTimeseries
from ..services.energysaving_runtime import (
    fetch_dashboard_alarms,
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


@router.get("/{plant}/alarms", response_model=List[AlarmEvent])
def plant_alarms(
    plant: str,
    room: Optional[str] = Query(None),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(100, lt=1000),
):
    del from_ts, to_ts, limit
    target = room or plant
    alarms = fetch_dashboard_alarms(target)
    LOGGER.debug("dashboard_refresh_alarms target=%s room=%s count=%s", plant, room, len(alarms))
    return alarms


@router.post("/{plant}/alarms")
def create_alarm(
    plant: str,
    payload: AlarmCreateIn,
):
    LOGGER.warning("dashboard_alarm_create_not_supported plant=%s signal=%s", plant, payload.signal)
    raise HTTPException(status_code=501, detail="Alarm creation is not supported on gfr_energysaving")
