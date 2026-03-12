from __future__ import annotations

"""Chunked backfill utility for pyramid aggregate tables.

Order of execution for sale:
    1min -> 15min -> 1h -> 1d -> 1month

Order of execution for compressori:
    1min -> 1h

Suggested long-history strategy:
    1min   -> recent period only (for example last 30-90 days)
    15min  -> medium period
    1h     -> at least from 2023 onward
    1d     -> long history
    1month -> full archive

Examples:
    python backend/scripts/backfill_aggregates.py --granularity 1min --from 2026-02-20 --to 2026-03-01 --truncate-target-range --chunk-unit day
    python backend/scripts/backfill_aggregates.py --granularity 15min --from 2026-02-01 --to 2026-03-01 --truncate-target-range --chunk-unit day
    python backend/scripts/backfill_aggregates.py --granularity 1h --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit week
    python backend/scripts/backfill_aggregates.py --granularity 1d --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit month
    python backend/scripts/backfill_aggregates.py --granularity 1month --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit month
    python backend/scripts/backfill_aggregates.py --dataset compressori --granularity 1h --from 2026-02-20 --to 2026-03-01 --truncate-target-range --chunk-unit day
"""

import argparse
import calendar
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        return False

from sqlalchemy import text
from sqlalchemy.engine import Engine

SCRIPT_PATH = Path(__file__).resolve()
BACKEND_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = BACKEND_ROOT.parent

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.aggregate_rollups import (  # noqa: E402
    DATASETS,
    build_delete_sql as build_shared_delete_sql,
    build_insert_sql as build_shared_insert_sql,
    dataset_granularities,
    level_spec_for_granularity,
    load_dataset_tables,
    refresh_aggregate_level_safely,
    validate_rollup_schema,
)
from app.scripts.csv_to_db_ingestor import resolve_database_url  # noqa: E402

LOGGER = logging.getLogger("backfill_aggregates")
DEFAULT_STATUS_FILE = BACKEND_ROOT / "runtime" / "backfill_aggregates.status.json"
SUPPORTED_DATASETS = tuple(DATASETS.keys())
SUPPORTED_GRANULARITIES = tuple(sorted({granularity for dataset_name in SUPPORTED_DATASETS for granularity in dataset_granularities(dataset_name)}))
SUPPORTED_CHUNK_UNITS = ("month", "week", "day")


@dataclass(frozen=True)
class BackfillConfig:
    dataset: str
    granularity: str
    range_start: datetime
    range_end: datetime
    sale_ids: list[int]
    resume: bool
    dry_run: bool
    truncate_target_range: bool
    chunk_unit: str
    chunk_size: int
    verbose: bool
    status_file: Path


@dataclass(frozen=True)
class ChunkWindow:
    start: datetime
    end: datetime


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def load_environment() -> None:
    load_dotenv(REPO_ROOT / ".env", override=False)
    load_dotenv(BACKEND_ROOT / ".env", override=False)


def _fallback_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return database_url

    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "3306").strip() or "3306"
    user = os.getenv("DB_USER", "").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    driver = os.getenv("DB_DRIVER", "mysql+pymysql").strip() or "mysql+pymysql"

    if all([host, user, password, name]):
        return f"{driver}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(name)}"

    raise RuntimeError(
        "Database configuration missing. Use project resolver, DATABASE_URL or DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME."
    )


def resolve_database_config() -> str:
    try:
        return resolve_database_url(None)
    except Exception as exc:
        LOGGER.debug("Falling back to direct DB env resolution: %s", exc)
        return _fallback_database_url()


def get_connection(database_url: str) -> Engine:
    from sqlalchemy import create_engine

    engine = create_engine(database_url, pool_pre_ping=True, future=True)
    dialect_name = engine.dialect.name.lower()
    if dialect_name != "mysql":
        raise RuntimeError(f"This backfill script requires MySQL-compatible SQL. Resolved dialect: {dialect_name!r}.")
    return engine


def parse_sale_ids(raw_value: str | None) -> list[int]:
    if not raw_value:
        return []
    sale_ids = sorted({int(part.strip()) for part in raw_value.split(",") if part.strip()})
    return sale_ids


def parse_date_argument(raw_value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} date {raw_value!r}. Expected YYYY-MM-DD.") from exc


def default_chunk_unit(dataset: str, granularity: str) -> str:
    if dataset == "compressori":
        return "day" if granularity == "1min" else "week"
    if granularity in {"1month", "1d", "1h"}:
        return "month"
    if granularity == "15min":
        return "week"
    return "day"


def validate_args(args: argparse.Namespace) -> BackfillConfig:
    range_start = parse_date_argument(args.from_date, "--from")
    range_end = parse_date_argument(args.to_date, "--to")
    if range_start >= range_end:
        raise ValueError("--to must be greater than --from. The upper bound is exclusive.")

    dataset = args.dataset
    granularity = args.granularity
    if granularity not in dataset_granularities(dataset):
        raise ValueError(
            f"Granularity {granularity!r} is not supported for dataset {dataset!r}. "
            f"Supported values: {', '.join(dataset_granularities(dataset))}."
        )

    chunk_unit = args.chunk_unit or default_chunk_unit(dataset, granularity)
    if chunk_unit not in SUPPORTED_CHUNK_UNITS:
        raise ValueError(f"Unsupported --chunk-unit {chunk_unit!r}.")

    if granularity == "1month" and chunk_unit != "month":
        raise ValueError("Granularity 1month supports only --chunk-unit month to avoid split monthly buckets.")

    if granularity == "1month" and (range_start.day != 1 or range_end.day != 1):
        raise ValueError("Granularity 1month requires --from and --to on the first day of a month.")

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1.")

    return BackfillConfig(
        dataset=dataset,
        granularity=granularity,
        range_start=range_start,
        range_end=range_end,
        sale_ids=parse_sale_ids(args.sale_ids),
        resume=bool(args.resume),
        dry_run=bool(args.dry_run),
        truncate_target_range=bool(args.truncate_target_range),
        chunk_unit=chunk_unit,
        chunk_size=int(args.chunk_size),
        verbose=bool(args.verbose),
        status_file=Path(args.status_file).resolve() if args.status_file else DEFAULT_STATUS_FILE,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill MySQL aggregate tables using hierarchical rollups instead of rebuilding every level from raw.",
    )
    parser.add_argument("--dataset", choices=SUPPORTED_DATASETS, default="sale")
    parser.add_argument("--granularity", choices=SUPPORTED_GRANULARITIES, required=True)
    parser.add_argument("--from", dest="from_date", required=True, help="Inclusive lower bound, format YYYY-MM-DD.")
    parser.add_argument("--to", dest="to_date", required=True, help="Exclusive upper bound, format YYYY-MM-DD.")
    parser.add_argument("--sale-ids", help="Optional comma-separated sale ids used as filter on idSala, for example 1,2,3.")
    parser.add_argument("--resume", action="store_true", help="Resume from the last completed chunk stored in the status file.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned SQL and chunks without writing data.")
    parser.add_argument("--truncate-target-range", action="store_true", help="Delete target buckets inside each chunk before re-inserting.")
    parser.add_argument("--chunk-unit", choices=SUPPORTED_CHUNK_UNITS, help="Chunk size unit. Defaults depend on dataset/granularity.")
    parser.add_argument("--chunk-size", type=int, default=1, help="Number of chunk units per block.")
    parser.add_argument("--status-file", help=f"Progress file path. Default: {DEFAULT_STATUS_FILE.as_posix()}")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")
    return parser


def load_progress(status_file: Path) -> dict[str, Any] | None:
    if not status_file.exists():
        return None
    return json.loads(status_file.read_text(encoding="utf-8"))


def save_progress(status_file: Path, payload: dict[str, Any]) -> None:
    status_file.parent.mkdir(parents=True, exist_ok=True)
    status_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def advance_datetime(value: datetime, chunk_unit: str, chunk_size: int) -> datetime:
    if chunk_unit == "month":
        return add_months(value, chunk_size)
    if chunk_unit == "week":
        return value + timedelta(weeks=chunk_size)
    if chunk_unit == "day":
        return value + timedelta(days=chunk_size)
    raise ValueError(f"Unsupported chunk unit {chunk_unit!r}.")


def iter_time_ranges(range_start: datetime, range_end: datetime, chunk_unit: str, chunk_size: int) -> list[ChunkWindow]:
    windows: list[ChunkWindow] = []
    current = range_start
    while current < range_end:
        next_value = min(advance_datetime(current, chunk_unit, chunk_size), range_end)
        windows.append(ChunkWindow(start=current, end=next_value))
        current = next_value
    return windows


def build_delete_sql(dataset: str, granularity: str, sale_ids: list[int] | None = None) -> tuple[str, dict[str, Any]]:
    return build_shared_delete_sql(dataset, granularity, sale_ids)


def build_insert_sql(
    dataset: str,
    granularity: str,
    tables: dict[str, Any],
    sale_ids: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    return build_shared_insert_sql(dataset, granularity, tables, sale_ids)


def render_sql_for_logging(sql: str, params: dict[str, Any]) -> str:
    compact_sql = "\n".join(line.rstrip() for line in sql.splitlines())
    return f"{compact_sql}\nparams={params}"


def status_identity(config: BackfillConfig) -> dict[str, Any]:
    return {
        "dataset": config.dataset,
        "granularity": config.granularity,
        "from": config.range_start.strftime("%Y-%m-%d"),
        "to": config.range_end.strftime("%Y-%m-%d"),
        "sale_ids": config.sale_ids,
        "chunk_unit": config.chunk_unit,
        "chunk_size": config.chunk_size,
    }


def validate_resume_state(config: BackfillConfig, payload: dict[str, Any]) -> datetime | None:
    identity = status_identity(config)
    mismatches: list[str] = []
    for key, expected_value in identity.items():
        if payload.get(key) != expected_value:
            mismatches.append(f"{key}: saved={payload.get(key)!r} cli={expected_value!r}")
    if mismatches:
        raise RuntimeError(
            "The saved progress file is not compatible with the requested resume:\n- " + "\n- ".join(mismatches)
        )

    last_completed_end = payload.get("last_completed_block_end")
    if not last_completed_end:
        return None
    return datetime.fromisoformat(last_completed_end)


def prepare_status_payload(config: BackfillConfig, target_table_name: str, source_table_name: str, chunk: ChunkWindow, rowcount: int) -> dict[str, Any]:
    payload = status_identity(config)
    payload.update(
        {
            "target_table": target_table_name,
            "source_table": source_table_name,
            "last_completed_block_start": chunk.start.isoformat(sep=" "),
            "last_completed_block_end": chunk.end.isoformat(sep=" "),
            "last_rowcount": rowcount,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return payload


def run_backfill(config: BackfillConfig) -> None:
    database_url = resolve_database_config()
    engine = get_connection(database_url)
    schema_warnings = validate_rollup_schema(engine)
    for warning in schema_warnings:
        LOGGER.warning("%s", warning)

    tables = load_dataset_tables(engine, config.dataset)
    level = level_spec_for_granularity(config.dataset, config.granularity)

    LOGGER.info(
        "Starting pyramid aggregate backfill dataset=%s granularity=%s source=%s target=%s from=%s to=%s sale_ids=%s chunk=%s:%s dry_run=%s truncate=%s",
        config.dataset,
        config.granularity,
        level.source_table,
        level.target_table,
        config.range_start.strftime("%Y-%m-%d"),
        config.range_end.strftime("%Y-%m-%d"),
        config.sale_ids or "ALL",
        config.chunk_unit,
        config.chunk_size,
        config.dry_run,
        config.truncate_target_range,
    )

    chunks = iter_time_ranges(config.range_start, config.range_end, config.chunk_unit, config.chunk_size)
    resume_from: datetime | None = None
    if config.resume:
        saved_progress = load_progress(config.status_file)
        if saved_progress is None:
            LOGGER.warning("Resume requested but no status file exists at %s. Starting from scratch.", config.status_file)
        else:
            resume_from = validate_resume_state(config, saved_progress)
            LOGGER.info("Resume enabled. Last completed chunk end=%s", resume_from.isoformat(sep=" ") if resume_from else "none")

    delete_sql, delete_params_template = build_delete_sql(config.dataset, config.granularity, config.sale_ids)
    insert_sql, insert_params_template = build_insert_sql(config.dataset, config.granularity, tables, config.sale_ids)

    if config.dry_run:
        preview_end = chunks[0].end if chunks else config.range_end
        preview_params = {
            "range_start": config.range_start,
            "range_end": min(preview_end, config.range_end),
            **{key: value for key, value in insert_params_template.items() if key not in {"range_start", "range_end"}},
        }
        if config.truncate_target_range:
            LOGGER.info("Dry run delete SQL:\n%s", render_sql_for_logging(delete_sql, {**delete_params_template, **preview_params}))
        LOGGER.info("Dry run insert SQL:\n%s", render_sql_for_logging(insert_sql, {**insert_params_template, **preview_params}))

    with engine.connect() as connection:
        for index, chunk in enumerate(chunks, start=1):
            if resume_from is not None and chunk.start < resume_from:
                LOGGER.info(
                    "Skipping chunk %s/%s because it is already completed: %s -> %s",
                    index,
                    len(chunks),
                    chunk.start,
                    chunk.end,
                )
                continue

            LOGGER.info(
                "Processing chunk %s/%s dataset=%s granularity=%s source=%s target=%s range=%s -> %s sale_ids=%s",
                index,
                len(chunks),
                config.dataset,
                config.granularity,
                level.source_table,
                level.target_table,
                chunk.start,
                chunk.end,
                config.sale_ids or "ALL",
            )

            transaction = connection.begin()
            try:
                result = refresh_aggregate_level_safely(
                    connection,
                    config.dataset,
                    config.granularity,
                    chunk.start,
                    chunk.end,
                    entity_ids=config.sale_ids,
                    truncate_target_range=config.truncate_target_range,
                    dry_run=config.dry_run,
                    tables=tables,
                    allow_upsert_fallback=True,
                    logger=LOGGER,
                )
                if not config.dry_run:
                    transaction.commit()
                    LOGGER.info(
                        "Chunk committed dataset=%s granularity=%s source=%s target=%s requested=%s -> %s expanded=%s -> %s deleted=%s affected=%s elapsed=%.2fs",
                        result.dataset,
                        result.granularity,
                        result.source_table,
                        result.target_table,
                        result.requested_start,
                        result.requested_end,
                        result.expanded_start,
                        result.expanded_end,
                        result.deleted_rows,
                        result.affected_rows,
                        result.elapsed_seconds,
                    )
                    save_progress(
                        config.status_file,
                        prepare_status_payload(config, result.target_table, result.source_table, chunk, result.affected_rows),
                    )
                else:
                    transaction.rollback()
                    LOGGER.info(
                        "Dry run chunk dataset=%s granularity=%s source=%s target=%s requested=%s -> %s expanded=%s -> %s elapsed=%.2fs",
                        result.dataset,
                        result.granularity,
                        result.source_table,
                        result.target_table,
                        result.requested_start,
                        result.requested_end,
                        result.expanded_start,
                        result.expanded_end,
                        result.elapsed_seconds,
                    )
            except Exception:
                transaction.rollback()
                LOGGER.exception("Chunk failed and was rolled back: %s -> %s", chunk.start, chunk.end)
                raise


def main() -> int:
    load_environment()
    parser = build_parser()
    try:
        args = parser.parse_args()
        config = validate_args(args)
        configure_logging(config.verbose)
        run_backfill(config)
    except KeyboardInterrupt:
        LOGGER.error("Interrupted by user.")
        return 130
    except Exception as exc:
        if not logging.getLogger().handlers:
            configure_logging(verbose=False)
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
