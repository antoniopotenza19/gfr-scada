import logging
import time
from datetime import datetime

from anyio import from_thread
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from ..config import settings
from ..db import get_db
from ..realtime_manager import manager
from ..services.csv_watcher import (
    build_day_source_url,
    ingest_remote_csv,
    list_remote_csv_files,
    resolve_plant_filename,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix='/api/ingest')


@router.get('/csv')
def ingest_csv(
    plant: str,
    date: str,
    base_url: str | None = None,
    db: Session = Depends(get_db),
):
    start_time = time.time()

    plant_row = db.query(models.Plant).filter(func.lower(models.Plant.name) == plant.lower()).first()
    if not plant_row:
        raise HTTPException(status_code=404, detail='Plant not found in registry')
    plant_name = plant_row.name

    try:
        target_day = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        raise HTTPException(status_code=400, detail='Invalid date format; use YYYY-MM-DD')

    source_root = (base_url or settings.base_csv_url).rstrip('/')
    source_url = build_day_source_url(source_root, target_day)
    try:
        files = list_remote_csv_files(source_url)
    except Exception as exc:
        logger.error('Could not list remote source %s: %s', source_url, exc)
        raise HTTPException(status_code=502, detail=f'Error listing CSV source: {exc}')

    filename = resolve_plant_filename(plant_name, target_day, files)
    if not filename:
        expected = f'{target_day:%m-%d}-{plant_name.upper().replace(" ", "_")}.csv'
        raise HTTPException(
            status_code=404,
            detail=f'CSV for plant not found in remote source (expected {expected})',
        )

    file_url = f'{source_url}/{filename}'
    logger.info('Manual ingest for plant=%s date=%s file=%s', plant_name, date, filename)

    try:
        result = ingest_remote_csv(
            db=db,
            plant=plant_name,
            file_url=file_url,
            filename=filename,
            since_ts=None,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error('Ingest failed for %s: %s', file_url, exc)
        raise HTTPException(status_code=400, detail=f'Error ingesting CSV: {exc}')

    try:
        from_thread.run(
            manager.broadcast_json,
            {
                'type': 'measurement_ingested',
                'plant': plant_name,
                'inserted': result.inserted,
                'ts': result.last_ts,
            },
        )
    except Exception:
        pass

    duration_ms = (time.time() - start_time) * 1000
    payload = {
        'file': filename,
        'source': 'remote',
        'bytes_read': result.bytes_read,
        'rows': result.rows_parsed,
        'signals': result.signals,
        'inserted': result.inserted,
        'skipped': result.skipped,
        'duration_ms': round(duration_ms, 2),
    }
    logger.info('Manual ingest complete: %s', payload)
    return payload
