from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from ..db import get_db

router = APIRouter(prefix='/api/debug')


def _to_iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace('+00:00', 'Z')


@router.get('/plant/{plant}/latest')
def plant_latest_debug(plant: str, db: Session = Depends(get_db)):
    now_utc = datetime.now(timezone.utc)
    cutoff_10 = now_utc - timedelta(minutes=10)
    cutoff_60 = now_utc - timedelta(minutes=60)

    db_max_ts = db.query(func.max(models.Measurement.ts)).filter(models.Measurement.plant == plant).scalar()
    db_rows_last_10_min = db.query(func.count(models.Measurement.id)).filter(
        models.Measurement.plant == plant,
        models.Measurement.ts >= cutoff_10,
    ).scalar() or 0
    db_rows_last_60_min = db.query(func.count(models.Measurement.id)).filter(
        models.Measurement.plant == plant,
        models.Measurement.ts >= cutoff_60,
    ).scalar() or 0

    return {
        'plant': plant,
        'db_max_ts': _to_iso_utc(db_max_ts),
        'db_rows_last_10_min': int(db_rows_last_10_min),
        'db_rows_last_60_min': int(db_rows_last_60_min),
        'now_utc': now_utc.isoformat().replace('+00:00', 'Z'),
    }
