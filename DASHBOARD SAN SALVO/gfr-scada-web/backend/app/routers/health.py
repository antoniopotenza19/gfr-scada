from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from .. import models
from ..db import get_db
from ..services import csv_watcher

router = APIRouter(prefix='/api')


@router.get('/health')
def api_health(db: Session = Depends(get_db)):
    db_connected = True
    measurements_latest_ts: dict[str, str | None] = {}
    try:
        db.execute(text('SELECT 1'))
    except Exception:
        db_connected = False

    if db_connected:
        try:
            latest_per_plant = (
                db.query(models.IngestState.plant, func.max(models.IngestState.last_ts))
                .group_by(models.IngestState.plant)
                .all()
            )
            measurements_latest_ts = {
                plant: ts.isoformat() if ts else None
                for plant, ts in latest_per_plant
            }
        except Exception:
            db_connected = False

    return {
        'db_connected': db_connected,
        'now_utc': datetime.now(timezone.utc).isoformat(),
        'measurements_latest_ts': measurements_latest_ts,
        'ingest_running': csv_watcher.is_running,
        'active_jobs_count': csv_watcher.active_jobs_count,
    }
