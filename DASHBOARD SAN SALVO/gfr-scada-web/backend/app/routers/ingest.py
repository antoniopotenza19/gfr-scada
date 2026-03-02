import os
import io
import re
import time
import logging
import requests
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from ..db import get_db
from .. import models

logger = logging.getLogger(__name__)
router = APIRouter(prefix='/api/ingest')


def _extract_unit_from_header(header: str) -> tuple[str, str]:
    """
    Extract signal name and unit from header like "Pressione (bar)".
    Returns (signal, unit) where unit is empty string if not found.
    """
    match = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', header.strip())
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return header.strip(), ''


def _parse_and_insert_wide_format(
    df: pd.DataFrame, plant: str, db: Session
) -> dict:
    """
    Transform wide-format CSV (many columns = signals) to long format and bulk insert.
    Returns dict with {rows_parsed, signals, inserted, skipped}.
    """
    # Detect timestamp column (typically first column)
    ts_col = df.columns[0]
    logger.info(f"Detected timestamp column: {ts_col}")

    # Parse timestamps with dayfirst=True (dd/MM/yyyy HH:mm:ss)
    try:
        df[ts_col] = pd.to_datetime(df[ts_col], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    except Exception as e:
        logger.warning(f"Failed to parse timestamp with format; trying flexible: {e}")
        df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=True, errors='coerce')

    # Identify data columns (everything except timestamp)
    data_cols = [c for c in df.columns if c != ts_col]
    logger.info(f"Found {len(data_cols)} potential signal columns")

    # Skip entirely empty columns
    data_cols = [c for c in data_cols if not df[c].isna().all()]
    logger.info(f"After removing empty columns: {len(data_cols)} signal columns")

    # Prepare bulk insert data
    measurements = []
    signal_names = set()
    skipped = 0

    for _, row in df.iterrows():
        ts = row[ts_col]
        # Skip rows with NaT timestamp
        if pd.isna(ts):
            skipped += 1
            continue

        for col in data_cols:
            value = row[col]
            # Skip NaN/None values
            if pd.isna(value) or value == '' or value is None:
                continue

            # Extract signal and unit from column header
            signal, unit = _extract_unit_from_header(col)
            signal_names.add(signal)

            # Try to convert value to float
            try:
                if isinstance(value, str):
                    # Replace comma with dot for decimal separator
                    value = float(value.replace(',', '.'))
                else:
                    value = float(value)
            except (ValueError, TypeError):
                skipped += 1
                continue

            measurements.append({
                'plant': plant,
                'room': None,  # Not available in current format
                'signal': signal,
                'value': value,
                'unit': unit,
                'ts': ts,
            })

    logger.info(f"Prepared {len(measurements)} measurements for insertion")

    # Bulk insert in chunks
    chunk_size = 10000
    inserted = 0
    for i in range(0, len(measurements), chunk_size):
        chunk = measurements[i:i + chunk_size]
        try:
            db.bulk_insert_mappings(models.Measurement, chunk)
            db.commit()
            inserted += len(chunk)
            logger.info(f"Inserted chunk {i//chunk_size + 1}: {len(chunk)} rows")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to insert chunk {i//chunk_size + 1}: {e}")
            raise

    return {
        'rows_parsed': len(df),
        'signals': len(signal_names),
        'inserted': inserted,
        'skipped': skipped,
    }




def _fetch_csv_url(url: str) -> pd.DataFrame:
    """
    Fetch and parse CSV from remote URL.
    """
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        raise HTTPException(status_code=502, detail=f'Error fetching CSV: {e}')

    # Try different encodings
    for encoding in ['latin-1', 'cp1252', 'utf-8']:
        try:
            text = r.content.decode(encoding)
            df = pd.read_csv(io.StringIO(text), sep=';', decimal=',')
            logger.info(f"Successfully parsed remote CSV with encoding {encoding}")
            return df
        except Exception as e:
            logger.debug(f"Failed to parse remote CSV with encoding {encoding}: {e}")
            continue

    raise HTTPException(
        status_code=400,
        detail='Could not parse remote CSV with any encoding'
    )


@router.get('/csv')
def ingest_csv(
    plant: str,
    date: str,
    base_url: str = None,
    db: Session = Depends(get_db),
):
    """
    Ingest CSV for a given plant and date (YYYY-MM-DD).
    Expected filename format: MM-DD-{PLANT_UPPERCASE}.CSV (e.g., "01-01-BRAVO.CSV")
    Expected remote URL structure: BASE_CSV_URL/YYYY/MM/DD/MM-DD-PLANT.CSV
    
    Parameters:
      - plant: Plant name (e.g., "BRAVO", "CENTAC") - will be converted to uppercase
      - date: Date in YYYY-MM-DD format
      - base_url: Override BASE_CSV_URL env var (optional)
    
    Returns JSON with file, rows, signals, inserted, skipped, duration_ms
    """
    start_time = time.time()


    # Parse date (YYYY-MM-DD format)
    parts = date.split('-')
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail='Invalid date format; use YYYY-MM-DD')
    y, m, d = parts

    # Build filename: MM-DD-{PLANT_UPPERCASE}.CSV
    plant_upper = plant.upper().replace(' ', '_')
    filename = f"{m}-{d}-{plant_upper}.CSV"
    logger.info(f"Looking for file: {filename} for date {date}")

    # always ingest from remote URL with hierarchical date path (YYYY/MM/DD/)
    base = base_url or os.getenv('BASE_CSV_URL')
    if not base:
        raise HTTPException(
            status_code=400,
            detail='No base URL configured'
        )
    url = f"{base}/{y}/{m}/{d}/{filename}"
    logger.info(f"Fetching remote CSV: {url}")
    df = _fetch_csv_url(url)
    source = 'remote'

    # Parse and insert
    logger.info(f"Starting to parse and insert from {source} CSV")
    stats = _parse_and_insert_wide_format(df, plant, db)

    duration_ms = (time.time() - start_time) * 1000
    result = {
        'file': filename,
        'source': source,
        'rows': stats['rows_parsed'],
        'signals': stats['signals'],
        'inserted': stats['inserted'],
        'skipped': stats['skipped'],
        'duration_ms': round(duration_ms, 2),
    }
    logger.info(f"Ingest complete: {result}")
    return result
