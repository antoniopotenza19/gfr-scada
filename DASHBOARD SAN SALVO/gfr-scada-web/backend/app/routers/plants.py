from datetime import datetime, timedelta, timezone
import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import Optional, List, Dict, Any
from anyio import from_thread
from ..db import get_db
from .. import models
from ..realtime_manager import manager
from ..schemas import PlantSummary, TimeseriesPoint, PlantTimeseries, AlarmEvent, AlarmCreateIn

router = APIRouter(prefix='/api/plants')
logger = logging.getLogger(__name__)
SUMMARY_LOOKBACK_MINUTES = 90


def to_iso_utc(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            value = parsed
        except Exception:
            return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace('+00:00', 'Z')


def empty_summary_payload(plant: str) -> dict[str, Any]:
    return {
        'plant': plant,
        'last_update': None,
        'signals': {},
        'compressors': [],
        'dryers': [],
        'active_alarms': [],
    }


@router.get('/', response_model=List[str])
def list_plants(db: Session = Depends(get_db)):
    plants = db.query(models.Plant.name).order_by(models.Plant.name).all()
    return [p[0] for p in plants]


@router.get('/{plant}/summary', response_model=PlantSummary)
def plant_summary(
    plant: str,
    room: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    plant_row = db.query(models.Plant).filter(func.lower(models.Plant.name) == plant.lower()).first()
    if not plant_row:
        raise HTTPException(status_code=404, detail='plant not found')
    plant_name = plant_row.name

    def row_get(row, key: str):
        if hasattr(row, key):
            return getattr(row, key)
        mapping = getattr(row, '_mapping', None)
        if mapping:
            return mapping.get(key)
        return None

    try:
        dialect_name = db.bind.dialect.name if db.bind else ''
        recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=SUMMARY_LOOKBACK_MINUTES)
        if dialect_name == 'postgresql':
            where_sql = 'WHERE plant = :plant AND ts >= :recent_cutoff'
            params: Dict[str, Any] = {'plant': plant_name, 'recent_cutoff': recent_cutoff}
            if room:
                where_sql += ' AND room = :room'
                params['room'] = room
            result = db.execute(
                text(
                    '''
                    SELECT DISTINCT ON (signal) signal, value, unit, ts
                    FROM measurements
                    '''
                    + where_sql
                    + '''
                    ORDER BY signal, ts DESC
                    '''
                ),
                params,
            )
            rows = result.fetchall()
        else:
            subq_query = db.query(
                models.Measurement.signal,
                func.max(models.Measurement.ts).label('ts')
            ).filter(
                models.Measurement.plant == plant_name,
                models.Measurement.ts >= recent_cutoff,
            )
            if room:
                subq_query = subq_query.filter(models.Measurement.room == room)
            subq = subq_query.group_by(models.Measurement.signal).subquery()

            rows_query = db.query(models.Measurement).join(
                subq,
                (models.Measurement.signal == subq.c.signal)
                & (models.Measurement.ts == subq.c.ts)
                & (models.Measurement.plant == plant_name)
            ).filter(models.Measurement.plant == plant_name)
            if room:
                rows_query = rows_query.filter(models.Measurement.room == room)
            rows = rows_query.all()
    except Exception:
        logger.exception('plant_summary query failed plant=%s room=%s', plant_name, room)
        raise HTTPException(status_code=500, detail='summary query failed')

    if not rows:
        logger.info(
            'plant_summary plant=%s room=%s no_recent_rows lookback_minutes=%s',
            plant_name,
            room,
            SUMMARY_LOOKBACK_MINUTES,
        )
        return empty_summary_payload(plant_name)

    parsed_rows: list[tuple[str, float, str, str]] = []
    for row in rows:
        signal = row_get(row, 'signal')
        value = row_get(row, 'value')
        unit = row_get(row, 'unit') or ''
        ts = row_get(row, 'ts')
        if signal is None or value is None or ts is None:
            continue
        serialized_ts = to_iso_utc(ts)
        if serialized_ts is None:
            continue
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            continue
        parsed_rows.append((signal, numeric_value, unit, serialized_ts))

    if not parsed_rows:
        return empty_summary_payload(plant_name)

    last_update = max(ts for _, _, _, ts in parsed_rows)
    signals = {}
    for signal, value, unit, ts in parsed_rows:
        signals[signal] = {'value': value, 'unit': unit, 'ts': ts}

    if not signals:
        return empty_summary_payload(plant_name)

    logger.info(
        'plant_summary plant=%s room=%s signals=%s max_ts=%s',
        plant_name,
        room,
        len(signals),
        to_iso_utc(last_update),
    )

    compressors = []
    dryers = []
    for sig, info in signals.items():
        value = float(info.get('value') or 0)
        if sig.upper().startswith('P-'):
            compressors.append({'id': sig, 'running': value > 0, 'local': False, 'fault': False})
        if sig.upper().startswith('A-'):
            dryers.append({'id': sig, 'running': value > 0, 'fault': False})

    try:
        alarm_query = db.query(models.AlarmEvent).filter(
            models.AlarmEvent.plant == plant_name,
            models.AlarmEvent.active.is_(True)
        )
        if room:
            alarm_query = alarm_query.filter(models.AlarmEvent.room == room)
        alarm_rows = alarm_query.order_by(models.AlarmEvent.ts.desc()).limit(50).all()
    except Exception:
        logger.exception('plant_summary alarm query failed plant=%s room=%s', plant_name, room)
        alarm_rows = []

    alarms = [
        {
            'code': a.signal,
            'severity': a.severity,
            'message': a.message,
            'ts': to_iso_utc(a.ts) or '',
        }
        for a in alarm_rows
    ]

    return {
        'plant': plant_name,
        'last_update': to_iso_utc(last_update),
        'signals': signals,
        'compressors': compressors,
        'dryers': dryers,
        'active_alarms': alarms,
    }


@router.get('/{plant}/timeseries', response_model=PlantTimeseries)
def plant_timeseries(
    plant: str,
    signal: str = Query(...),
    room: Optional[str] = Query(None),
    minutes: int = Query(60, ge=1),
    max_points: int = Query(5000, ge=1, le=20000),
    from_ts: Optional[str] = Query(None, alias='from'),
    to_ts: Optional[str] = Query(None, alias='to'),
    bucket: Optional[str] = None,
    agg: str = Query('avg', regex='^(avg|sum|min|max)$'),
    db: Session = Depends(get_db),
):
    plant_row = db.query(models.Plant).filter(func.lower(models.Plant.name) == plant.lower()).first()
    if not plant_row:
        raise HTTPException(status_code=404, detail='plant not found')
    plant_name = plant_row.name

    now_utc = datetime.now(timezone.utc)
    cutoff_ts = now_utc - timedelta(minutes=minutes)

    params: Dict[str, Any] = {'plant': plant_name, 'signal': signal, 'cutoff_ts': cutoff_ts, 'max_points': max_points}
    conditions = ['plant = :plant', 'signal = :signal']
    if room:
        conditions.append('room = :room')
        params['room'] = room
    if from_ts or to_ts:
        if from_ts:
            conditions.append('ts >= :from_ts')
            params['from_ts'] = from_ts
        if to_ts:
            conditions.append('ts <= :to_ts')
            params['to_ts'] = to_ts
    else:
        conditions.append('ts >= :cutoff_ts')

    cond_sql = ' AND '.join(conditions)
    if bucket:
        agg_fn = agg.lower()
        sql = 'SELECT ts, value FROM ('
        sql += f'SELECT time_bucket(:bucket, ts) AS ts, {agg_fn}(value) AS value '
        sql += f'FROM measurements WHERE {cond_sql} GROUP BY 1 ORDER BY 1 DESC LIMIT :max_points'
        sql += ') AS recent ORDER BY ts'
        params['bucket'] = bucket
    else:
        sql = f'SELECT ts, value FROM (SELECT ts, value FROM measurements WHERE {cond_sql} ORDER BY ts DESC LIMIT :max_points) AS recent ORDER BY ts'

    result = db.execute(text(sql), params)
    points: List[TimeseriesPoint] = []
    for row in result.fetchall():
        if row.ts is None or row.value is None:
            continue
        ts_iso = to_iso_utc(row.ts)
        if ts_iso is None:
            continue
        points.append({'ts': ts_iso, 'value': float(row.value)})

    max_ts = points[-1]['ts'] if points else None
    logger.info(
        'plant_timeseries plant=%s room=%s signal=%s minutes=%s points=%s max_ts=%s',
        plant_name,
        room,
        signal,
        minutes,
        len(points),
        max_ts,
    )

    return {'plant': plant_name, 'signal': signal, 'points': points}


@router.get('/{plant}/alarms', response_model=List[AlarmEvent])
def plant_alarms(
    plant: str,
    room: Optional[str] = Query(None),
    from_ts: Optional[str] = Query(None, alias='from'),
    to_ts: Optional[str] = Query(None, alias='to'),
    limit: int = Query(100, lt=1000),
    db: Session = Depends(get_db),
):
    plant_row = db.query(models.Plant).filter(func.lower(models.Plant.name) == plant.lower()).first()
    if not plant_row:
        raise HTTPException(status_code=404, detail='plant not found')
    plant_name = plant_row.name

    q = db.query(models.AlarmEvent).filter(models.AlarmEvent.plant == plant_name)
    if room:
        q = q.filter(models.AlarmEvent.room == room)
    if from_ts:
        q = q.filter(models.AlarmEvent.ts >= from_ts)
    if to_ts:
        q = q.filter(models.AlarmEvent.ts <= to_ts)

    rows = q.order_by(models.AlarmEvent.ts.desc()).limit(limit).all()
    return [
        {
            'code': r.signal,
            'severity': r.severity,
            'message': r.message,
            'ts': to_iso_utc(r.ts) or '',
        }
        for r in rows
    ]


@router.post('/{plant}/alarms')
def create_alarm(
    plant: str,
    payload: AlarmCreateIn,
    db: Session = Depends(get_db),
):
    plant_row = db.query(models.Plant).filter(func.lower(models.Plant.name) == plant.lower()).first()
    if not plant_row:
        raise HTTPException(status_code=404, detail='plant not found')
    plant_name = plant_row.name

    alarm = models.AlarmEvent(
        plant=plant_name,
        room=payload.room,
        signal=payload.signal,
        severity=payload.severity,
        message=payload.message,
        active=True,
        ts=datetime.utcnow(),
    )
    db.add(alarm)
    db.commit()
    db.refresh(alarm)

    try:
        from_thread.run(
            manager.broadcast_json,
            {
                'type': 'alarm_created',
                'plant': plant_name,
                'code': alarm.signal,
                'severity': alarm.severity,
                'message': alarm.message,
                'ts': alarm.ts,
            },
        )
    except Exception:
        pass

    return {'id': alarm.id}
