from __future__ import annotations

"""Chunked backfill utility for aggregate SCADA tables.

Examples:
    python backend/scripts/backfill_aggregates.py --granularity 1d --from 2024-11-01 --to 2025-03-01
    python backend/scripts/backfill_aggregates.py --granularity 1h --from 2024-11-01 --to 2025-03-01 --sale-ids 1,2,3
    python backend/scripts/backfill_aggregates.py --granularity 1min --from 2025-02-01 --to 2025-03-01 --chunk-unit day
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
from time import perf_counter
from typing import Any
from urllib.parse import quote_plus

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional convenience dependency
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        return False
from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import Table

LOGGER = logging.getLogger("backfill_aggregates")

SCRIPT_PATH = Path(__file__).resolve()
BACKEND_ROOT = SCRIPT_PATH.parents[1]
REPO_ROOT = BACKEND_ROOT.parent
DEFAULT_STATUS_FILE = BACKEND_ROOT / "runtime" / "backfill_aggregates.status.json"

SUPPORTED_GRANULARITIES = ("1month", "1d", "1h", "15min", "1min")
SUPPORTED_CHUNK_UNITS = ("month", "week", "day")


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    raw_table: str
    target_tables: dict[str, str]
    entity_id_column: str
    raw_timestamp_column: str = "timestamp"


@dataclass(frozen=True)
class AggregateTargetLayout:
    table_name: str
    entity_id_column: str
    bucket_column: str
    insert_columns: tuple[str, ...]
    update_columns: tuple[str, ...]
    insert_expressions: dict[str, str]
    bucket_expression_alias: str


RAW_METRIC_COLUMNS = (
    "pressione",
    "potAttTotale",
    "flusso",
    "dewpoint",
    "temperatura",
    "umidita_relativa",
    "consSpecifico",
    "totMetriCubi",
)


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


SALE_DATASET = DatasetSpec(
    name="sale",
    raw_table="registrazioni_sale",
    target_tables={
        "1month": "sale_agg_1month",
        "1d": "sale_agg_1d",
        "1h": "sale_agg_1h",
        "15min": "sale_agg_15min",
        "1min": "sale_agg_1min",
    },
    entity_id_column="idSala",
)

DATASETS: dict[str, DatasetSpec] = {
    SALE_DATASET.name: SALE_DATASET,
}

TARGET_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "entity_id": ("idSala", "sala_id"),
    "bucket_start": ("bucket_start", "timestamp", "bucket_ts", "period_start", "interval_start", "time_bucket"),
    "source_min_ts": ("source_min_ts", "raw_min_ts", "range_start", "bucket_source_start", "min_timestamp"),
    "source_max_ts": ("source_max_ts", "raw_max_ts", "range_end", "bucket_source_end", "max_timestamp"),
    "sample_count": ("sample_count", "samples", "num_samples", "row_count"),
    "bucket_end": ("bucket_end", "period_end", "interval_end"),
    "duration_hours": ("duration_hours", "bucket_hours"),
    "energia_kwh": ("energia_kwh", "energy_kwh", "energia", "kwh", "consumo_kwh"),
    "volume_mc": ("volume_mc", "volume_m3", "volume", "metri_cubi", "totMetriCubi", "delta_totMetriCubi"),
    "pressione_avg": ("pressione", "avg_pressione", "pressione_avg"),
    "potAttTotale_avg": ("potAttTotale", "avg_potAttTotale", "potAttTotale_avg"),
    "flusso_avg": ("flusso", "avg_flusso", "flusso_avg"),
    "dewpoint_avg": ("dewpoint", "avg_dewpoint", "dewpoint_avg"),
    "temperatura_avg": ("temperatura", "avg_temperatura", "temperatura_avg"),
    "umidita_relativa_avg": ("umidita_relativa", "avg_umidita_relativa", "umidita_relativa_avg"),
    "consSpecifico_avg": ("consSpecifico", "avg_consSpecifico", "consSpecifico_avg"),
    "created_at": ("created_at",),
    "updated_at": ("updated_at",),
}


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

    missing = [env_name for env_name, value in {
        "DATABASE_URL": database_url,
        "DB_HOST": host,
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_NAME": name,
    }.items() if not value]
    raise RuntimeError(
        "Database configuration missing. Reuse the project helper or configure DATABASE_URL or DB_* env vars "
        f"(missing: {', '.join(missing)})."
    )


def resolve_database_config() -> str:
    if str(BACKEND_ROOT) not in sys.path:
        sys.path.insert(0, str(BACKEND_ROOT))

    try:
        from app.scripts.csv_to_db_ingestor import resolve_database_url as project_resolve_database_url

        database_url = project_resolve_database_url(None)
        LOGGER.debug("Using project DB resolver from app.scripts.csv_to_db_ingestor.")
        return database_url
    except Exception as exc:
        LOGGER.debug("Falling back to local DB env resolution: %s", exc)
        return _fallback_database_url()


def get_connection(database_url: str) -> Engine:
    engine = create_engine(database_url, pool_pre_ping=True, future=True)
    dialect_name = engine.dialect.name.lower()
    if dialect_name != "mysql":
        raise RuntimeError(
            f"This backfill script requires MySQL-compatible SQL. Resolved dialect: {dialect_name!r}."
        )
    return engine


def parse_sale_ids(raw_value: str | None) -> list[int]:
    if not raw_value:
        return []

    sale_ids: list[int] = []
    for part in raw_value.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        sale_ids.append(int(stripped))
    return sorted(set(sale_ids))


def parse_date_argument(raw_value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} date {raw_value!r}. Expected YYYY-MM-DD.") from exc


def default_chunk_unit(granularity: str) -> str:
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

    granularity = args.granularity
    chunk_unit = args.chunk_unit or default_chunk_unit(granularity)
    if chunk_unit not in SUPPORTED_CHUNK_UNITS:
        raise ValueError(f"Unsupported --chunk-unit {chunk_unit!r}.")

    if granularity == "1month" and chunk_unit != "month":
        raise ValueError("Granularity 1month supports only --chunk-unit month to avoid split monthly buckets.")

    if granularity == "1month" and (range_start.day != 1 or range_end.day != 1):
        raise ValueError("Granularity 1month requires --from and --to on the first day of a month.")

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1.")

    return BackfillConfig(
        dataset="sale",
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
        description="Backfill MySQL aggregate tables from raw SCADA history with chunked INSERT ... SELECT queries.",
    )
    parser.add_argument("--granularity", choices=SUPPORTED_GRANULARITIES, required=True)
    parser.add_argument("--from", dest="from_date", required=True, help="Inclusive lower bound, format YYYY-MM-DD.")
    parser.add_argument("--to", dest="to_date", required=True, help="Exclusive upper bound, format YYYY-MM-DD.")
    parser.add_argument("--sale-ids", help="Optional comma-separated room ids, for example 1,2,3.")
    parser.add_argument("--resume", action="store_true", help="Resume from the last completed chunk stored in the status file.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned SQL and chunks without writing data.")
    parser.add_argument(
        "--truncate-target-range",
        action="store_true",
        help="Delete target aggregate rows in each chunk before inserting them again.",
    )
    parser.add_argument("--chunk-unit", choices=SUPPORTED_CHUNK_UNITS, help="Chunk size unit. Defaults depend on granularity.")
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


def ensure_tables(engine: Engine, dataset: DatasetSpec, granularity: str) -> tuple[Table, Table]:
    target_table_name = dataset.target_tables[granularity]
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[dataset.raw_table, target_table_name])
    missing = [name for name in (dataset.raw_table, target_table_name) if name not in metadata.tables]
    if missing:
        raise RuntimeError(f"Missing required tables in target DB: {', '.join(missing)}")
    raw_table = metadata.tables[dataset.raw_table]
    required_raw_columns = {dataset.entity_id_column, dataset.raw_timestamp_column}
    missing_raw_columns = sorted(required_raw_columns.difference(raw_table.columns.keys()))
    if missing_raw_columns:
        raise RuntimeError(
            f"Raw table {raw_table.name} is missing required columns: {', '.join(missing_raw_columns)}."
        )
    return raw_table, metadata.tables[target_table_name]


def bucket_expression_sql(granularity: str, source_alias: str) -> str:
    timestamp_column = f"{source_alias}.timestamp"
    if granularity == "1month":
        return f"STR_TO_DATE(DATE_FORMAT({timestamp_column}, '%Y-%m-01 00:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "1d":
        return f"STR_TO_DATE(DATE_FORMAT({timestamp_column}, '%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "1h":
        return f"STR_TO_DATE(DATE_FORMAT({timestamp_column}, '%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "15min":
        return f"FROM_UNIXTIME(UNIX_TIMESTAMP({timestamp_column}) - MOD(UNIX_TIMESTAMP({timestamp_column}), 900))"
    if granularity == "1min":
        return f"FROM_UNIXTIME(UNIX_TIMESTAMP({timestamp_column}) - MOD(UNIX_TIMESTAMP({timestamp_column}), 60))"
    raise ValueError(f"Unsupported granularity {granularity!r}.")


def bucket_end_expression_sql(granularity: str, bucket_alias: str) -> str:
    if granularity == "1month":
        return f"DATE_ADD({bucket_alias}, INTERVAL 1 MONTH)"
    if granularity == "1d":
        return f"DATE_ADD({bucket_alias}, INTERVAL 1 DAY)"
    if granularity == "1h":
        return f"DATE_ADD({bucket_alias}, INTERVAL 1 HOUR)"
    if granularity == "15min":
        return f"DATE_ADD({bucket_alias}, INTERVAL 15 MINUTE)"
    if granularity == "1min":
        return f"DATE_ADD({bucket_alias}, INTERVAL 1 MINUTE)"
    raise ValueError(f"Unsupported granularity {granularity!r}.")


def bucket_duration_hours_sql(granularity: str, bucket_alias: str) -> str:
    return f"(TIMESTAMPDIFF(SECOND, {bucket_alias}, {bucket_end_expression_sql(granularity, bucket_alias)}) / 3600.0)"


def resolve_target_column_logical_name(column_name: str) -> str | None:
    for logical_name, aliases in TARGET_COLUMN_ALIASES.items():
        if column_name in aliases:
            return logical_name
    return None


def build_expression_catalog(raw_table: Table, granularity: str) -> dict[str, str]:
    raw_columns = set(raw_table.columns.keys())
    bucket_alias = "bucket_start"
    bucket_expr = bucket_expression_sql(granularity, "src")
    duration_expr = bucket_duration_hours_sql(granularity, bucket_alias)

    expressions: dict[str, str] = {
        "entity_id": f"src.{SALE_DATASET.entity_id_column}",
        "bucket_start": bucket_alias,
        "source_min_ts": "MIN(src.timestamp)",
        "source_max_ts": "MAX(src.timestamp)",
        "sample_count": "COUNT(*)",
        "bucket_end": bucket_end_expression_sql(granularity, bucket_alias),
        "duration_hours": duration_expr,
        "created_at": "CURRENT_TIMESTAMP",
        "updated_at": "CURRENT_TIMESTAMP",
        "_bucket_subquery_expr": bucket_expr,
    }

    average_columns = {
        "pressione_avg": "pressione",
        "potAttTotale_avg": "potAttTotale",
        "flusso_avg": "flusso",
        "dewpoint_avg": "dewpoint",
        "temperatura_avg": "temperatura",
        "umidita_relativa_avg": "umidita_relativa",
        "consSpecifico_avg": "consSpecifico",
    }
    for logical_name, raw_column in average_columns.items():
        if raw_column in raw_columns:
            expressions[logical_name] = f"AVG(src.{raw_column})"

    if "totMetriCubi" in raw_columns:
        expressions["volume_mc"] = "GREATEST(COALESCE(MAX(src.totMetriCubi) - MIN(src.totMetriCubi), 0), 0)"

    if "potAttTotale" in raw_columns:
        expressions["energia_kwh"] = f"(AVG(src.potAttTotale) * {duration_expr})"

    return expressions


def resolve_target_layout(raw_table: Table, target_table: Table, granularity: str) -> AggregateTargetLayout:
    insert_expressions = build_expression_catalog(raw_table, granularity)
    bucket_expression_alias = insert_expressions.pop("_bucket_subquery_expr")

    entity_id_column: str | None = None
    bucket_column: str | None = None
    insert_columns: list[str] = []
    update_columns: list[str] = []
    unresolved_required_columns: list[str] = []

    for column in target_table.columns:
        logical_name = resolve_target_column_logical_name(column.name)
        if logical_name == "entity_id":
            entity_id_column = column.name
        elif logical_name == "bucket_start":
            bucket_column = column.name

        if logical_name is None or logical_name not in insert_expressions:
            if (
                not column.nullable
                and column.server_default is None
                and column.default is None
                and not getattr(column, "autoincrement", False)
            ):
                unresolved_required_columns.append(column.name)
            continue

        insert_columns.append(column.name)
        if column.name not in {entity_id_column, bucket_column} and logical_name not in {"created_at"}:
            update_columns.append(column.name)

    if entity_id_column is None or bucket_column is None:
        raise RuntimeError(
            f"Target table {target_table.name} must expose an entity id column and a bucket timestamp column. "
            f"Known aliases: {TARGET_COLUMN_ALIASES['entity_id']} / {TARGET_COLUMN_ALIASES['bucket_start']}."
        )

    unresolved_required_columns = [
        column_name for column_name in unresolved_required_columns if column_name not in insert_columns
    ]
    if unresolved_required_columns:
        raise RuntimeError(
            f"Target table {target_table.name} has required columns that this script cannot populate yet: "
            f"{', '.join(unresolved_required_columns)}."
        )

    resolved_insert_expressions: dict[str, str] = {}
    for column_name in insert_columns:
        logical_name = resolve_target_column_logical_name(column_name)
        if logical_name is None:
            continue
        resolved_insert_expressions[column_name] = insert_expressions[logical_name]

    return AggregateTargetLayout(
        table_name=target_table.name,
        entity_id_column=entity_id_column,
        bucket_column=bucket_column,
        insert_columns=tuple(insert_columns),
        update_columns=tuple(update_columns),
        insert_expressions=resolved_insert_expressions,
        bucket_expression_alias=bucket_expression_alias,
    )


def build_sale_filter_sql(sale_ids: list[int], column_sql: str = "src.idSala", bind_prefix: str = "sale_id") -> tuple[str, dict[str, int]]:
    if not sale_ids:
        return "", {}
    placeholders = ", ".join(f":{bind_prefix}_{index}" for index, _ in enumerate(sale_ids))
    params = {f"{bind_prefix}_{index}": sale_id for index, sale_id in enumerate(sale_ids)}
    return f" AND {column_sql} IN ({placeholders})", params


def build_delete_sql(layout: AggregateTargetLayout, granularity: str, sale_ids: list[int] | None = None) -> tuple[str, dict[str, Any]]:
    del granularity
    sale_filter_sql, sale_params = build_sale_filter_sql(sale_ids or [], column_sql=layout.entity_id_column)
    sql = (
        f"DELETE FROM `{layout.table_name}` "
        f"WHERE `{layout.bucket_column}` >= :range_start AND `{layout.bucket_column}` < :range_end"
        f"{sale_filter_sql}"
    )
    params: dict[str, Any] = {
        "range_start": None,
        "range_end": None,
        **sale_params,
    }
    return sql, params


def build_raw_subquery_select_sql(dataset: DatasetSpec, raw_table: Table, bucket_expression_sql_fragment: str) -> str:
    available_columns = set(raw_table.columns.keys())
    selected_columns = [
        f"src.`{dataset.entity_id_column}`",
        f"src.`{dataset.raw_timestamp_column}` AS `timestamp`",
    ]
    for column_name in RAW_METRIC_COLUMNS:
        if column_name in available_columns:
            selected_columns.append(f"src.`{column_name}`")
    selected_columns.append(f"{bucket_expression_sql_fragment} AS bucket_start")
    return ",\n        ".join(selected_columns)


def build_insert_sql(
    dataset: DatasetSpec,
    raw_table: Table,
    layout: AggregateTargetLayout,
    granularity: str,
    sale_ids: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    del granularity
    sale_filter_sql, sale_params = build_sale_filter_sql(sale_ids or [], column_sql=f"src.{dataset.entity_id_column}")
    insert_columns_sql = ", ".join(f"`{column_name}`" for column_name in layout.insert_columns)
    select_columns_sql = ",\n       ".join(
        f"{layout.insert_expressions[column_name]} AS `{column_name}`" for column_name in layout.insert_columns
    )
    raw_subquery_select_sql = build_raw_subquery_select_sql(dataset, raw_table, layout.bucket_expression_alias)
    update_sql = ",\n    ".join(
        f"`{column_name}` = VALUES(`{column_name}`)" for column_name in layout.update_columns
    )
    sql = f"""
INSERT INTO `{layout.table_name}` (
    {insert_columns_sql}
)
SELECT
       {select_columns_sql}
FROM (
    SELECT
        {raw_subquery_select_sql}
    FROM `{dataset.raw_table}` AS src
    WHERE src.`{dataset.raw_timestamp_column}` >= :range_start
      AND src.`{dataset.raw_timestamp_column}` < :range_end{sale_filter_sql}
) AS src
GROUP BY src.`{dataset.entity_id_column}`, bucket_start
"""
    if update_sql:
        sql = f"{sql}ON DUPLICATE KEY UPDATE\n    {update_sql}"
    params: dict[str, Any] = {
        "range_start": None,
        "range_end": None,
        **sale_params,
    }
    return sql.strip(), params


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


def prepare_status_payload(
    config: BackfillConfig,
    target_table_name: str,
    chunk: ChunkWindow,
    rowcount: int,
) -> dict[str, Any]:
    payload = status_identity(config)
    payload.update(
        {
            "target_table": target_table_name,
            "last_completed_block_start": chunk.start.isoformat(sep=" "),
            "last_completed_block_end": chunk.end.isoformat(sep=" "),
            "last_rowcount": rowcount,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return payload


def warn_if_target_may_duplicate(engine: Engine, layout: AggregateTargetLayout) -> None:
    inspector = inspect(engine)
    key_candidates: list[set[str]] = []
    pk_constraint = inspector.get_pk_constraint(layout.table_name)
    pk_columns = set(pk_constraint.get("constrained_columns") or [])
    if pk_columns:
        key_candidates.append(pk_columns)

    for constraint in inspector.get_unique_constraints(layout.table_name):
        columns = set(constraint.get("column_names") or [])
        if columns:
            key_candidates.append(columns)

    for index in inspector.get_indexes(layout.table_name):
        if not index.get("unique"):
            continue
        columns = set(index.get("column_names") or [])
        if columns:
            key_candidates.append(columns)

    required_key = {layout.entity_id_column, layout.bucket_column}
    if not any(required_key.issubset(candidate) for candidate in key_candidates):
        LOGGER.warning(
            "Target table %s does not expose an obvious unique key on (%s, %s). "
            "Reruns without --truncate-target-range may duplicate rows.",
            layout.table_name,
            layout.entity_id_column,
            layout.bucket_column,
        )


def run_backfill(config: BackfillConfig) -> None:
    dataset = DATASETS[config.dataset]
    database_url = resolve_database_config()
    engine = get_connection(database_url)
    raw_table, target_table = ensure_tables(engine, dataset, config.granularity)
    layout = resolve_target_layout(raw_table, target_table, config.granularity)
    warn_if_target_may_duplicate(engine, layout)

    LOGGER.info(
        "Starting aggregate backfill dataset=%s granularity=%s target=%s from=%s to=%s sale_ids=%s chunk=%s:%s dry_run=%s truncate=%s",
        config.dataset,
        config.granularity,
        target_table.name,
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
            LOGGER.info(
                "Resume enabled. Last completed chunk end=%s", resume_from.isoformat(sep=" ") if resume_from else "none"
            )

    delete_sql, delete_params_template = build_delete_sql(layout, config.granularity, config.sale_ids)
    insert_sql, insert_params_template = build_insert_sql(
        dataset,
        raw_table,
        layout,
        config.granularity,
        config.sale_ids,
    )

    if config.dry_run:
        preview_params = {
            "range_start": config.range_start,
            "range_end": min(chunks[0].end if chunks else config.range_end, config.range_end),
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
                "Processing chunk %s/%s granularity=%s range=%s -> %s sale_ids=%s",
                index,
                len(chunks),
                config.granularity,
                chunk.start,
                chunk.end,
                config.sale_ids or "ALL",
            )
            started_at = perf_counter()

            chunk_delete_params = {
                **delete_params_template,
                "range_start": chunk.start,
                "range_end": chunk.end,
            }
            chunk_insert_params = {
                **insert_params_template,
                "range_start": chunk.start,
                "range_end": chunk.end,
            }

            if config.dry_run:
                LOGGER.info(
                    "Dry run chunk %s/%s completed without DB writes in %.2fs.",
                    index,
                    len(chunks),
                    perf_counter() - started_at,
                )
                continue

            transaction = connection.begin()
            try:
                deleted_rows = 0
                if config.truncate_target_range:
                    deleted_rows = connection.execute(text(delete_sql), chunk_delete_params).rowcount or 0

                result = connection.execute(text(insert_sql), chunk_insert_params)
                affected_rows = result.rowcount or 0
                transaction.commit()
                elapsed = perf_counter() - started_at
                LOGGER.info(
                    "Chunk committed range=%s -> %s deleted=%s affected=%s elapsed=%.2fs",
                    chunk.start,
                    chunk.end,
                    deleted_rows,
                    affected_rows,
                    elapsed,
                )
                save_progress(
                    config.status_file,
                    prepare_status_payload(config, target_table.name, chunk, affected_rows),
                )
            except SQLAlchemyError:
                transaction.rollback()
                LOGGER.exception("Chunk failed and was rolled back: %s -> %s", chunk.start, chunk.end)
                raise
            except Exception:
                transaction.rollback()
                LOGGER.exception("Unexpected error. Chunk rolled back: %s -> %s", chunk.start, chunk.end)
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
