import re
from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models
from ..db import get_db
from ..services import csv_watcher

router = APIRouter(prefix='/api/ingest')
SOURCE_DATE_RE = re.compile(r'/(\d{4})/(\d{2})/(\d{2})/?$')


def _source_date(source_url: str | None) -> str | None:
    if not source_url:
        return None
    match = SOURCE_DATE_RE.search(source_url)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
    except Exception:
        return None


@router.get('/status')
def ingest_status(plant: str | None = None, db: Session = Depends(get_db)):
    query = db.query(models.IngestState).order_by(models.IngestState.plant, models.IngestState.filename)
    if plant:
        query = query.filter(models.IngestState.plant.ilike(f'%{plant}%'))

    runtime_rows = csv_watcher.runtime_jobs(plant=plant)
    runtime_by_key = {
        (row['source_url'], row['plant'], row['filename']): row
        for row in runtime_rows
    }

    states = []
    seen_keys: set[tuple[str, str, str]] = set()
    for row in query.all():
        key = (row.source_url, row.plant, row.filename)
        runtime = runtime_by_key.get(key, {})
        seen_keys.add(key)
        states.append(
            {
                'source_url': row.source_url,
                'folder_url': runtime.get('folder_url') or row.source_url,
                'computed_date': runtime.get('computed_date') or _source_date(row.source_url),
                'plant': row.plant,
                'filename': row.filename,
                'last_modified': row.last_modified,
                'last_byte_offset': row.last_byte_offset,
                'last_ts': row.last_ts,
                'updated_at': row.updated_at,
                'last_success_ts': runtime.get('last_success_ts'),
                'last_inserted_ts': runtime.get('last_inserted_ts') or runtime.get('latest_ts_inserted'),
                'last_insert_count': runtime.get('last_insert_count', 0),
                'last_error': runtime.get('last_error'),
                'last_error_ts': runtime.get('last_error_ts'),
                'last_rows_parsed': runtime.get('last_rows_parsed', 0),
                'last_bytes_read': runtime.get('last_bytes_read', 0),
                'last_bytes_delta': runtime.get('last_bytes_delta', 0),
                'latest_ts_inserted': runtime.get('latest_ts_inserted'),
                'last_cycle_ts': runtime.get('last_cycle_ts'),
                'no_progress_cycles': runtime.get('no_progress_cycles', 0),
            }
        )

    for key, runtime in runtime_by_key.items():
        if key in seen_keys:
            continue
        source_url, row_plant, filename = key
        states.append(
            {
                'source_url': source_url,
                'folder_url': runtime.get('folder_url') or source_url,
                'computed_date': runtime.get('computed_date') or _source_date(source_url),
                'plant': row_plant,
                'filename': filename,
                'last_modified': None,
                'last_byte_offset': None,
                'last_ts': None,
                'updated_at': None,
                'last_success_ts': runtime.get('last_success_ts'),
                'last_inserted_ts': runtime.get('last_inserted_ts') or runtime.get('latest_ts_inserted'),
                'last_insert_count': runtime.get('last_insert_count', 0),
                'last_error': runtime.get('last_error'),
                'last_error_ts': runtime.get('last_error_ts'),
                'last_rows_parsed': runtime.get('last_rows_parsed', 0),
                'last_bytes_read': runtime.get('last_bytes_read', 0),
                'last_bytes_delta': runtime.get('last_bytes_delta', 0),
                'latest_ts_inserted': runtime.get('latest_ts_inserted'),
                'last_cycle_ts': runtime.get('last_cycle_ts'),
                'no_progress_cycles': runtime.get('no_progress_cycles', 0),
            }
        )

    return {
        'running': csv_watcher.is_running,
        'active_jobs_count': csv_watcher.active_jobs_count,
        'count': len(states),
        'items': states,
    }


@router.get('/sources')
def ingest_sources():
    return csv_watcher.sources()


@router.post('/start')
async def ingest_start():
    started = await csv_watcher.start()
    return {'running': csv_watcher.is_running, 'started': started}


@router.post('/stop')
async def ingest_stop():
    stopped = await csv_watcher.stop()
    return {'running': csv_watcher.is_running, 'stopped': stopped}
