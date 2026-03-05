import asyncio
import io
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
from urllib.parse import unquote

import pandas as pd
import requests
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .. import models
from ..config import settings
from ..db import SessionLocal

logger = logging.getLogger(__name__)

CSV_LINK_RE = re.compile(r'href=[\'"]([^\'"]+?\.csv)[\'"]', re.IGNORECASE)


@dataclass
class IngestResult:
    source_url: str
    filename: str
    rows_parsed: int
    signals: int
    inserted: int
    skipped: int
    last_ts: datetime | None
    last_modified: datetime | None
    bytes_read: int


def normalize_plant_token(plant: str) -> str:
    return plant.strip().upper().replace(' ', '_')


def _normalize_plant_lookup(plant: str) -> str:
    return re.sub(r'[\s_]+', '', plant).upper()


def build_day_source_url(base_csv_url: str, target_day: date) -> str:
    return f"{base_csv_url.rstrip('/')}/{target_day:%Y/%m/%d}"


def build_day_filename_for_plant(plant: str, target_day: date) -> str:
    return f'{target_day:%m-%d}-{normalize_plant_token(plant)}.csv'


def list_remote_csv_files(day_source_url: str) -> list[str]:
    resp = requests.get(f'{day_source_url}/', timeout=10)
    resp.raise_for_status()
    raw_links = CSV_LINK_RE.findall(resp.text)

    files: set[str] = set()
    for raw in raw_links:
        name = unquote(raw.split('?', 1)[0].split('#', 1)[0]).strip('/')
        if not name:
            continue
        basename = name.rsplit('/', 1)[-1]
        if basename.lower().endswith('.csv'):
            files.add(basename)
    return sorted(files, key=str.lower)


def extract_plant_from_filename(filename: str, target_day: date | None = None) -> str | None:
    basename = filename.rsplit('/', 1)[-1].strip()
    if not basename.lower().endswith('.csv'):
        return None

    stem = basename[:-4].strip()
    if not stem:
        return None

    prefix = f'{target_day:%m-%d}-' if target_day else None
    if prefix and stem.upper().startswith(prefix.upper()):
        plant = stem[len(prefix):]
    else:
        match = re.match(r'^\d{2}-\d{2}-(.+)$', stem)
        plant = match.group(1) if match else stem

    plant = re.sub(r'\s+', ' ', plant).strip()
    return plant or None


def extract_plants_from_filenames(files: list[str], target_day: date | None = None) -> list[str]:
    plants: list[str] = []
    seen: set[str] = set()
    for filename in files:
        plant = extract_plant_from_filename(filename, target_day=target_day)
        if not plant:
            continue
        key = _normalize_plant_lookup(plant)
        if key in seen:
            continue
        seen.add(key)
        plants.append(plant)
    return plants


def resolve_plant_filename(plant: str, target_day: date, files: list[str]) -> str | None:
    target_key = _normalize_plant_lookup(plant)
    for name in files:
        discovered = extract_plant_from_filename(name, target_day=target_day)
        if discovered and _normalize_plant_lookup(discovered) == target_key:
            return name

    expected_root = build_day_filename_for_plant(plant, target_day).rsplit('.', 1)[0].upper()
    for name in files:
        if name.rsplit('.', 1)[0].upper() == expected_root:
            return name
    return None


def parse_http_last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_remote_last_modified(file_url: str) -> datetime | None:
    try:
        resp = requests.head(file_url, timeout=10, allow_redirects=True)
        resp.raise_for_status()
    except Exception:
        return None
    return parse_http_last_modified(resp.headers.get('Last-Modified'))


def _extract_unit_from_header(header: str) -> tuple[str, str]:
    raw = header.strip()
    index_suffix: str | None = None

    # Handles headers like "Pressione (bar) (2)".
    explicit_index = re.search(r'\((\d+)\)\s*$', raw)
    if explicit_index:
        index_suffix = explicit_index.group(1)
        raw = raw[:explicit_index.start()].strip()
    else:
        # Handles pandas duplicate-column suffixes like ".1", ".2".
        duplicate_suffix = re.search(r'\.(\d+)\s*$', raw)
        if duplicate_suffix:
            index_suffix = str(int(duplicate_suffix.group(1)) + 1)
            raw = raw[:duplicate_suffix.start()].strip()

    match = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', raw)
    if match:
        signal = match.group(1).strip()
        unit = match.group(2).strip()
    else:
        signal = raw
        unit = ''

    if index_suffix:
        signal = f'{signal} ({index_suffix})'

    return signal, unit


def _as_naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def fetch_csv_dataframe(url: str) -> tuple[pd.DataFrame, datetime | None, int]:
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    last_modified = parse_http_last_modified(resp.headers.get('Last-Modified'))
    bytes_read = len(resp.content)

    for encoding in ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']:
        try:
            text = resp.content.decode(encoding)
            frame = pd.read_csv(io.StringIO(text), sep=';', decimal=',')
            return frame, last_modified, bytes_read
        except Exception:
            continue
    raise ValueError('Could not parse remote CSV with any known encoding')


def _build_measurements(df: pd.DataFrame, plant: str, since_ts: datetime | None) -> tuple[list[dict], dict]:
    if df.empty:
        return [], {'rows_parsed': 0, 'signals': 0, 'skipped': 0, 'last_ts': None}

    ts_col = df.columns[0]
    try:
        df[ts_col] = pd.to_datetime(df[ts_col], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    except Exception:
        df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=True, errors='coerce')

    data_cols = [c for c in df.columns if c != ts_col and not df[c].isna().all()]
    df = df.dropna(subset=[ts_col])

    if since_ts is not None:
        normalized_since = _as_naive_utc(since_ts)
        if normalized_since is not None:
            df = df[df[ts_col] > normalized_since]

    measurements: list[dict] = []
    signal_names: set[str] = set()
    skipped = 0
    last_ts: datetime | None = None

    for _, row in df.iterrows():
        ts = row[ts_col]
        if pd.isna(ts):
            skipped += 1
            continue

        ts_val = _as_naive_utc(ts.to_pydatetime())
        last_ts = ts_val if last_ts is None or (ts_val and ts_val > last_ts) else last_ts

        for col in data_cols:
            value = row[col]
            if pd.isna(value) or value == '' or value is None:
                continue

            signal, unit = _extract_unit_from_header(col)
            signal_names.add(signal)
            try:
                number = float(value.replace(',', '.')) if isinstance(value, str) else float(value)
            except (ValueError, TypeError):
                skipped += 1
                continue

            measurements.append(
                {
                    'plant': plant,
                    'room': None,
                    'signal': signal,
                    'value': number,
                    'unit': unit,
                    'ts': ts_val,
                }
            )

    return measurements, {
        'rows_parsed': len(df),
        'signals': len(signal_names),
        'skipped': skipped,
        'last_ts': last_ts,
    }


def _insert_measurements_idempotent(db: Session, measurements: list[dict], chunk_size: int = 10000) -> int:
    if not measurements:
        return 0

    dialect = db.bind.dialect.name if db.bind else 'sqlite'
    inserted = 0

    for i in range(0, len(measurements), chunk_size):
        chunk = measurements[i:i + chunk_size]
        if dialect == 'postgresql':
            stmt = pg_insert(models.Measurement).values(chunk).on_conflict_do_nothing(
                index_elements=['plant', 'signal', 'ts']
            )
        else:
            stmt = sqlite_insert(models.Measurement).values(chunk).on_conflict_do_nothing(
                index_elements=['plant', 'signal', 'ts']
            )
        result = db.execute(stmt)
        if result.rowcount and result.rowcount > 0:
            inserted += result.rowcount

    return inserted


def ingest_remote_csv(
    db: Session,
    plant: str,
    file_url: str,
    filename: str,
    since_ts: datetime | None = None,
) -> IngestResult:
    frame, last_modified, bytes_read = fetch_csv_dataframe(file_url)
    measurements, stats = _build_measurements(frame, plant, since_ts)
    inserted = _insert_measurements_idempotent(db, measurements)

    return IngestResult(
        source_url=file_url.rsplit('/', 1)[0],
        filename=filename,
        rows_parsed=stats['rows_parsed'],
        signals=stats['signals'],
        inserted=inserted,
        skipped=stats['skipped'],
        last_ts=stats['last_ts'],
        last_modified=last_modified,
        bytes_read=bytes_read,
    )


class CsvWatcher:
    def __init__(
        self,
        session_factory: sessionmaker,
        base_csv_url: str,
        poll_seconds: int,
        plants: list[str] | None = None,
    ):
        self._session_factory = session_factory
        self.base_csv_url = base_csv_url.rstrip('/')
        self.poll_seconds = max(1, poll_seconds)
        self.plants = [p for p in (plants or []) if p]
        self.source_tz = ZoneInfo(settings.ingest_source_timezone)
        self._task: asyncio.Task | None = None
        self._enabled = False
        self._lock = asyncio.Lock()
        self._active_jobs_count = 0
        self._job_runtime: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._no_progress_warn_cycles = 3

    def _source_today(self) -> date:
        return datetime.now(self.source_tz).date()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done() and self._enabled

    @property
    def active_jobs_count(self) -> int:
        return self._active_jobs_count

    async def start(self) -> bool:
        async with self._lock:
            if self.is_running:
                return False
            self._enabled = True
            self._task = asyncio.create_task(self._run_loop(), name='csv-watcher')
            return True

    async def stop(self) -> bool:
        async with self._lock:
            if not self._task:
                self._enabled = False
                return False
            self._enabled = False
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            return True

    def sources(self) -> dict:
        today = self._source_today()
        return {
            'base_csv_url': self.base_csv_url,
            'today': today.isoformat(),
            'today_source_url': build_day_source_url(self.base_csv_url, today),
            'poll_seconds': self.poll_seconds,
            'configured_plants': self.plants,
            'source_timezone': settings.ingest_source_timezone,
            'running': self.is_running,
        }

    @staticmethod
    def _to_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def runtime_jobs(self, plant: str | None = None) -> list[dict[str, Any]]:
        normalized = plant.lower() if plant else None
        rows: list[dict[str, Any]] = []
        for key in sorted(self._job_runtime.keys(), key=lambda k: (k[1], k[2], k[0])):
            source_url, row_plant, filename = key
            if normalized and normalized not in row_plant.lower():
                continue
            row = self._job_runtime[key]
            rows.append(
                {
                    'source_url': source_url,
                    'folder_url': row.get('folder_url') or source_url,
                    'computed_date': row.get('computed_date'),
                    'plant': row_plant,
                    'filename': filename,
                    'last_success_ts': self._to_iso(row.get('last_success_ts')),
                    'last_insert_count': row.get('last_insert_count'),
                    'last_error': row.get('last_error'),
                    'last_error_ts': self._to_iso(row.get('last_error_ts')),
                    'last_rows_parsed': row.get('last_rows_parsed'),
                    'last_bytes_read': row.get('last_bytes_read'),
                    'last_bytes_delta': row.get('last_bytes_delta'),
                    'latest_ts_inserted': self._to_iso(row.get('latest_ts_inserted')),
                    'last_inserted_ts': self._to_iso(row.get('latest_ts_inserted')),
                    'last_cycle_ts': self._to_iso(row.get('last_cycle_ts')),
                    'no_progress_cycles': row.get('no_progress_cycles', 0),
                }
            )
        return rows

    def _runtime_row(self, source_url: str, plant: str, filename: str) -> dict[str, Any]:
        key = (source_url, plant, filename)
        row = self._job_runtime.get(key)
        if row is None:
            row = {
                'last_success_ts': None,
                'last_insert_count': 0,
                'last_error': None,
                'last_error_ts': None,
                'last_rows_parsed': 0,
                'last_bytes_read': 0,
                'last_bytes_delta': 0,
                'latest_ts_inserted': None,
                'last_cycle_ts': None,
                'no_progress_cycles': 0,
                'folder_url': source_url,
                'computed_date': None,
            }
            self._job_runtime[key] = row
        return row

    def _record_job_success(
        self,
        source_url: str,
        computed_day: date,
        plant: str,
        filename: str,
        result: IngestResult,
        bytes_delta: int,
    ) -> dict[str, Any]:
        runtime = self._runtime_row(source_url, plant, filename)
        runtime['last_success_ts'] = datetime.now(timezone.utc)
        runtime['last_insert_count'] = result.inserted
        runtime['last_rows_parsed'] = result.rows_parsed
        runtime['last_bytes_read'] = result.bytes_read
        runtime['last_bytes_delta'] = bytes_delta
        runtime['latest_ts_inserted'] = result.last_ts
        runtime['last_cycle_ts'] = datetime.now(timezone.utc)
        runtime['last_error'] = None
        runtime['last_error_ts'] = None
        runtime['folder_url'] = source_url
        runtime['computed_date'] = computed_day.isoformat()
        return runtime

    def _record_job_error(
        self,
        source_url: str,
        computed_day: date,
        plant: str,
        filename: str,
        exc: Exception,
    ):
        runtime = self._runtime_row(source_url, plant, filename)
        runtime['last_error'] = str(exc)
        runtime['last_error_ts'] = datetime.now(timezone.utc)
        runtime['last_cycle_ts'] = datetime.now(timezone.utc)
        runtime['folder_url'] = source_url
        runtime['computed_date'] = computed_day.isoformat()

    def _record_job_skip(self, source_url: str, computed_day: date, plant: str, filename: str):
        runtime = self._runtime_row(source_url, plant, filename)
        runtime['last_cycle_ts'] = datetime.now(timezone.utc)
        runtime['folder_url'] = source_url
        runtime['computed_date'] = computed_day.isoformat()

    async def _run_loop(self):
        while self._enabled:
            started = time.monotonic()
            try:
                await asyncio.to_thread(self.run_cycle_once)
            except Exception:
                logger.exception('CSV watcher cycle failed')
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.2, self.poll_seconds - elapsed))

    def _resolve_plants(self, db: Session) -> list[str]:
        rows = db.query(models.Plant.name).order_by(models.Plant.name).all()
        db_plants = [name for (name,) in rows]
        if not self.plants:
            return db_plants

        allowlist = {p.lower() for p in self.plants}
        return [name for name in db_plants if name.lower() in allowlist]

    def _sync_plants_from_files(self, db: Session, computed_day: date, files: list[str]) -> list[str]:
        discovered = extract_plants_from_filenames(files, target_day=computed_day)
        if not discovered:
            return []

        existing_rows = db.query(models.Plant.name).all()
        existing = {_normalize_plant_lookup(name): name for (name,) in existing_rows}
        added: list[str] = []
        for plant in discovered:
            key = _normalize_plant_lookup(plant)
            if key in existing:
                continue
            db.add(models.Plant(name=plant))
            existing[key] = plant
            added.append(plant)

        if added:
            db.flush()
            logger.info(
                'csv_ingest_auto_sync_plants added=%s plants=%s',
                len(added),
                ','.join(added),
            )
        return added

    def _get_or_create_state(self, db: Session, source_url: str, plant: str, filename: str):
        state = (
            db.query(models.IngestState)
            .filter(
                models.IngestState.source_url == source_url,
                models.IngestState.plant == plant,
                models.IngestState.filename == filename,
            )
            .first()
        )
        if state:
            return state
        state = models.IngestState(
            source_url=source_url,
            plant=plant,
            filename=filename,
            last_modified=None,
            last_byte_offset=None,
            last_ts=None,
        )
        db.add(state)
        db.flush()
        return state

    def _process_file(self, db: Session, source_url: str, computed_day: date, plant: str, filename: str):
        file_url = f'{source_url}/{filename}'
        state = self._get_or_create_state(db, source_url, plant, filename)
        prev_last_modified = state.last_modified
        prev_byte_offset = state.last_byte_offset
        remote_last_modified = get_remote_last_modified(file_url)

        if state.last_modified and remote_last_modified and remote_last_modified <= state.last_modified:
            self._record_job_skip(source_url, computed_day, plant, filename)
            return

        result = ingest_remote_csv(
            db=db,
            plant=plant,
            file_url=file_url,
            filename=filename,
            since_ts=state.last_ts,
        )

        if result.last_ts is not None:
            state.last_ts = result.last_ts
        state.last_modified = result.last_modified or remote_last_modified or state.last_modified
        bytes_delta = result.bytes_read - int(prev_byte_offset or 0)
        state.last_byte_offset = result.bytes_read
        state.updated_at = datetime.utcnow()
        runtime = self._record_job_success(source_url, computed_day, plant, filename, result, bytes_delta)

        modified_advanced = bool(
            remote_last_modified and prev_last_modified and remote_last_modified > prev_last_modified
        )
        no_new_bytes = (
            prev_byte_offset is not None and result.bytes_read is not None and result.bytes_read <= prev_byte_offset
        )
        no_new_rows = result.rows_parsed <= 0 or result.inserted <= 0
        if modified_advanced and (no_new_bytes or no_new_rows):
            runtime['no_progress_cycles'] = int(runtime.get('no_progress_cycles', 0)) + 1
            if runtime['no_progress_cycles'] >= self._no_progress_warn_cycles:
                logger.warning(
                    'csv_ingest_no_progress plant=%s file=%s last_modified=%s bytes_read=%s rows_parsed=%s rows_inserted=%s no_progress_cycles=%s',
                    plant,
                    filename,
                    state.last_modified,
                    result.bytes_read,
                    result.rows_parsed,
                    result.inserted,
                    runtime['no_progress_cycles'],
                )
        else:
            runtime['no_progress_cycles'] = 0

        logger.info(
            'csv_ingest_cycle plant=%s folder_url=%s filename=%s last_modified=%s bytes_delta=%s rows_inserted=%s',
            plant,
            source_url,
            filename,
            state.last_modified,
            bytes_delta,
            result.inserted,
        )

    def _list_source_with_fallback(self) -> tuple[date, str, list[str]] | None:
        source_day = self._source_today()
        source_url = build_day_source_url(self.base_csv_url, source_day)
        try:
            files = list_remote_csv_files(source_url)
            if files:
                return source_day, source_url, files
            logger.warning('CSV watcher found empty folder for today: %s', source_url)
        except Exception as exc:
            logger.warning('Could not list today CSV directory %s: %s', source_url, exc)

        fallback_day = source_day - timedelta(days=1)
        fallback_url = build_day_source_url(self.base_csv_url, fallback_day)
        logger.warning('Falling back to yesterday folder: %s', fallback_url)
        try:
            fallback_files = list_remote_csv_files(fallback_url)
        except Exception as exc:
            logger.warning('Could not list fallback CSV directory %s: %s', fallback_url, exc)
            return None
        if not fallback_files:
            logger.warning('Fallback folder is empty: %s', fallback_url)
            return None
        return fallback_day, fallback_url, fallback_files

    def run_cycle_once(self):
        self._active_jobs_count = 0
        logger.info(
            'csv_ingest_cycle_start source_today=%s source_tz=%s now_utc=%s',
            self._source_today().isoformat(),
            settings.ingest_source_timezone,
            datetime.now(timezone.utc).isoformat(),
        )

        source_info = self._list_source_with_fallback()
        if source_info is None:
            return

        computed_day, source_url, files = source_info

        db = self._session_factory()
        try:
            try:
                added = self._sync_plants_from_files(db, computed_day, files)
                if added:
                    db.commit()
            except Exception:
                db.rollback()
                logger.exception(
                    'csv_ingest_auto_sync_plants_failed folder_url=%s computed_day=%s',
                    source_url,
                    computed_day.isoformat(),
                )

            plants = self._resolve_plants(db)
            jobs: list[tuple[str, str]] = []
            for plant in plants:
                filename = resolve_plant_filename(plant, computed_day, files)
                if not filename:
                    continue
                jobs.append((plant, filename))

            self._active_jobs_count = len(jobs)
            for plant, filename in jobs:
                try:
                    self._process_file(db, source_url, computed_day, plant, filename)
                    db.commit()
                except Exception as exc:
                    db.rollback()
                    self._record_job_error(source_url, computed_day, plant, filename, exc)
                    logger.exception(
                        'csv_ingest_error plant=%s file=%s source_url=%s',
                        plant,
                        filename,
                        source_url,
                    )
                    logger.warning('Skipping file plant=%s filename=%s reason=%s', plant, filename, exc)
        finally:
            db.close()


csv_watcher = CsvWatcher(
    session_factory=SessionLocal,
    base_csv_url=settings.base_csv_url,
    poll_seconds=settings.ingest_poll_seconds,
    plants=settings.ingest_plants,
)
