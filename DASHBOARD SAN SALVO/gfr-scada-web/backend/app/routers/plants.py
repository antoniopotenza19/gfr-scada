from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import Optional, List, Dict, Any
from ..db import get_db
from .. import models

router = APIRouter(prefix="/api/plants")

# helper service functions moved to separate module

@router.get("/", response_model=List[str])
def list_plants(db: Session = Depends(get_db)):
    plants = db.query(models.Measurement.plant).distinct().order_by(models.Measurement.plant).all()
    return [p[0] for p in plants]


from ..schemas import PlantSummary, TimeseriesPoint, AlarmEvent

@router.get("/{plant}/summary", response_model=PlantSummary)
def plant_summary(
    plant: str,
    db: Session = Depends(get_db),
):
    # latest timestamp per signal
    subq = db.query(
        models.Measurement.signal,
        func.max(models.Measurement.ts).label("ts")
    ).filter(models.Measurement.plant == plant).group_by(models.Measurement.signal).subquery()

    rows = db.query(models.Measurement).join(
        subq,
        (models.Measurement.signal == subq.c.signal)
        & (models.Measurement.ts == subq.c.ts)
    ).all()

    if not rows:
        raise HTTPException(status_code=404, detail="plant not found or no data")

    last_update = max(r.ts for r in rows)
    signals = {r.signal: {"value": r.value, "unit": r.unit, "ts": r.ts} for r in rows}

    # simple static mapping stub for compressors/dryers based on signal naming
    compressors = []
    dryers = []
    for sig, info in signals.items():
        if sig.upper().startswith("P-"):
            compressors.append({"id": sig, "running": info["value"] > 0, "local": False, "fault": False})
        if sig.upper().startswith("A-"):
            dryers.append({"id": sig, "running": info["value"] > 0, "fault": False})

    # dummy active_alarms from alarms endpoint
    alarms = []  # frontend will call alarms separately if desired

    return {
        "plant": plant,
        "last_update": last_update,
        "signals": signals,
        "compressors": compressors,
        "dryers": dryers,
        "active_alarms": alarms,
    }


@router.get("/{plant}/timeseries", response_model=List[TimeseriesPoint])
def plant_timeseries(
    plant: str,
    signal: str = Query(...),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    bucket: Optional[str] = None,
    db: Session = Depends(get_db),
):
    sql = None
    params: Dict[str, Any] = {"plant": plant, "signal": signal}
    conditions = ["plant = :plant", "signal = :signal"]
    if from_ts:
        conditions.append("ts >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts:
        conditions.append("ts <= :to_ts")
        params["to_ts"] = to_ts

    cond_sql = " AND ".join(conditions)
    if bucket:
        sql = f"SELECT time_bucket(:bucket, ts) AS ts, avg(value) AS value "
        sql += f"FROM measurements WHERE {cond_sql} GROUP BY ts ORDER BY ts"
        params["bucket"] = bucket
    else:
        sql = f"SELECT ts, value FROM measurements WHERE {cond_sql} ORDER BY ts"
    result = db.execute(text(sql), params)
    return [{"ts": row[0], "value": row[1]} for row in result.fetchall()]


@router.get("/{plant}/alarms", response_model=List[AlarmEvent])
def plant_alarms(
    plant: str,
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(100, lt=1000),
    db: Session = Depends(get_db),
):
    q = db.query(models.Measurement).filter(models.Measurement.plant == plant)
    if from_ts:
        q = q.filter(models.Measurement.ts >= from_ts)
    if to_ts:
        q = q.filter(models.Measurement.ts <= to_ts)
    # simplistic alarm filter
    q = q.filter(
        (models.Measurement.signal.ilike('%ALARM%'))
        | (models.Measurement.signal.ilike('XA-%'))
    )
    q = q.order_by(models.Measurement.ts.desc()).limit(limit)
    rows = q.all()
    events = []
    for r in rows:
        events.append({
            "code": r.signal,
            "severity": 'high' if 'XA' in r.signal.upper() else 'low',
            "message": f"Signal {r.signal} alarm",
            "ts": r.ts,
        })
    return events
