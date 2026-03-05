"""
Backfill remote CSV data into measurements for a date range.

Examples:
  python app/scripts/backfill_remote_csv.py
  python app/scripts/backfill_remote_csv.py --start 2025-03-01 --end 2026-03-02
  python app/scripts/backfill_remote_csv.py --plants BRAVO,SS1,SS2
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import func

load_dotenv()

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db import SessionLocal
from app.models import Plant
from app.services.csv_watcher import (
    build_day_source_url,
    ingest_remote_csv,
    list_remote_csv_files,
    normalize_plant_token,
)


def parse_date(value: str) -> date:
    return datetime.strptime(value, '%Y-%m-%d').date()


def parse_plant_from_filename(filename: str) -> str | None:
    stem = filename.rsplit('.', 1)[0]
    if len(stem) < 7:
        return None
    # Expected: MM-DD-<PLANT_TOKEN>
    if stem[2:3] != '-' or stem[5:6] != '-':
        return None
    plant = stem[6:].strip()
    return plant or None


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def get_or_create_plant_name(db, raw_name: str) -> str:
    existing = (
        db.query(Plant)
        .filter(func.lower(Plant.name) == raw_name.lower())
        .first()
    )
    if existing:
        return existing.name

    db.add(Plant(name=raw_name))
    db.flush()
    return raw_name


def main():
    today = date.today()
    default_start = today - timedelta(days=365)

    parser = argparse.ArgumentParser(description='Backfill remote CSV range into measurements.')
    parser.add_argument('--start', default=default_start.isoformat(), help='Start date YYYY-MM-DD (inclusive).')
    parser.add_argument('--end', default=today.isoformat(), help='End date YYYY-MM-DD (inclusive).')
    parser.add_argument(
        '--base-url',
        default=os.getenv('BASE_CSV_URL', 'http://94.138.172.234:46812/shared'),
        help='Remote base CSV URL.',
    )
    parser.add_argument(
        '--plants',
        default='',
        help='Optional comma-separated plant names/tokens to include.',
    )
    parser.add_argument(
        '--max-days',
        type=int,
        default=0,
        help='Optional safety limit on processed days (0 = no limit).',
    )

    args = parser.parse_args()
    start = parse_date(args.start)
    end = parse_date(args.end)
    if end < start:
        raise SystemExit('Invalid range: end < start')

    allow_tokens = {
        normalize_plant_token(item)
        for item in [v.strip() for v in args.plants.split(',') if v.strip()]
    }

    print(f'Backfill start={start} end={end} base_url={args.base_url}')
    if allow_tokens:
        print(f'Plant filter={sorted(allow_tokens)}')

    db = SessionLocal()
    try:
        day_count = 0
        files_seen = 0
        files_ingested = 0
        rows_inserted = 0

        for day in daterange(start, end):
            day_count += 1
            if args.max_days > 0 and day_count > args.max_days:
                print(f'Stopping due to --max-days={args.max_days}')
                break

            source_url = build_day_source_url(args.base_url, day)
            try:
                files = list_remote_csv_files(source_url)
            except Exception as exc:
                print(f'[{day}] skip: cannot list source ({exc})')
                continue

            if not files:
                continue

            print(f'[{day}] files={len(files)}')
            for filename in files:
                files_seen += 1
                raw_plant = parse_plant_from_filename(filename)
                if not raw_plant:
                    continue

                token = normalize_plant_token(raw_plant)
                if allow_tokens and token not in allow_tokens:
                    continue

                try:
                    plant_name = get_or_create_plant_name(db, raw_plant)
                    result = ingest_remote_csv(
                        db=db,
                        plant=plant_name,
                        file_url=f'{source_url}/{filename}',
                        filename=filename,
                        since_ts=None,
                    )
                    db.commit()
                    files_ingested += 1
                    rows_inserted += max(0, int(result.inserted or 0))
                    print(
                        f'  ok {plant_name:24} file={filename} inserted={result.inserted} '
                        f'rows={result.rows_parsed} signals={result.signals}'
                    )
                except Exception as exc:
                    db.rollback()
                    print(f'  err file={filename} reason={exc}')

        print('')
        print('Backfill completed')
        print(f'  days_processed={day_count}')
        print(f'  files_seen={files_seen}')
        print(f'  files_ingested={files_ingested}')
        print(f'  rows_inserted={rows_inserted}')
    finally:
        db.close()


if __name__ == '__main__':
    main()
