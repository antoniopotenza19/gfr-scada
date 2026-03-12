from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any

from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import Table

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AggregateLevelSpec:
    granularity: str
    target_table: str
    source_table: str
    source_kind: str
    source_time_column: str


@dataclass(frozen=True)
class AggregateDatasetSpec:
    name: str
    raw_table: str
    filter_column: str
    group_columns: tuple[str, ...]
    levels: tuple[AggregateLevelSpec, ...]


@dataclass(frozen=True)
class RefreshLevelResult:
    dataset: str
    granularity: str
    source_table: str
    target_table: str
    requested_start: datetime
    requested_end: datetime
    expanded_start: datetime
    expanded_end: datetime
    deleted_rows: int
    affected_rows: int
    elapsed_seconds: float


SALE_LEVELS: tuple[AggregateLevelSpec, ...] = (
    AggregateLevelSpec("1min", "sale_agg_1min", "registrazioni_sale", "raw", "timestamp"),
    AggregateLevelSpec("15min", "sale_agg_15min", "sale_agg_1min", "rollup", "bucket_start"),
    AggregateLevelSpec("1h", "sale_agg_1h", "sale_agg_15min", "rollup", "bucket_start"),
    AggregateLevelSpec("1d", "sale_agg_1d", "sale_agg_1h", "rollup", "bucket_start"),
    AggregateLevelSpec("1month", "sale_agg_1month", "sale_agg_1d", "rollup", "bucket_start"),
)

COMPRESSORI_LEVELS: tuple[AggregateLevelSpec, ...] = (
    AggregateLevelSpec("1min", "compressori_agg_1min", "registrazioni_compressori", "raw", "timestamp"),
    AggregateLevelSpec("1h", "compressori_agg_1h", "compressori_agg_1min", "rollup", "bucket_start"),
)

SALE_DATASET = AggregateDatasetSpec(
    name="sale",
    raw_table="registrazioni_sale",
    filter_column="idSala",
    group_columns=("idSala",),
    levels=SALE_LEVELS,
)

COMPRESSORI_DATASET = AggregateDatasetSpec(
    name="compressori",
    raw_table="registrazioni_compressori",
    filter_column="idSala",
    group_columns=("idCompressore", "idSala"),
    levels=COMPRESSORI_LEVELS,
)

DATASETS: dict[str, AggregateDatasetSpec] = {
    SALE_DATASET.name: SALE_DATASET,
    COMPRESSORI_DATASET.name: COMPRESSORI_DATASET,
}

ROLLUP_REQUIRED_COLUMNS: dict[str, dict[str, set[str]]] = {
    "sale": {
        "1min": {
            "idSala",
            "bucket_start",
            "samples_count",
            "volume_nm3_sum",
            "energia_kwh_sum",
            "pressione_avg",
            "pressione_min",
            "pressione_max",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "flusso_nm3h_avg",
            "flusso_nm3h_min",
            "flusso_nm3h_max",
            "dewpoint_avg",
            "dewpoint_min",
            "dewpoint_max",
            "temperatura_avg",
            "temperatura_min",
            "temperatura_max",
            "umidita_relativa_avg",
            "cons_specifico_avg",
        },
        "15min": {
            "idSala",
            "bucket_start",
            "samples_count",
            "volume_nm3_sum",
            "energia_kwh_sum",
            "pressione_avg",
            "pressione_min",
            "pressione_max",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "flusso_nm3h_avg",
            "flusso_nm3h_min",
            "flusso_nm3h_max",
            "dewpoint_avg",
            "dewpoint_min",
            "dewpoint_max",
            "temperatura_avg",
            "temperatura_min",
            "temperatura_max",
            "umidita_relativa_avg",
            "cons_specifico_avg",
        },
        "1h": {
            "idSala",
            "bucket_start",
            "samples_count",
            "volume_nm3_sum",
            "energia_kwh_sum",
            "pressione_avg",
            "pressione_min",
            "pressione_max",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "flusso_nm3h_avg",
            "flusso_nm3h_min",
            "flusso_nm3h_max",
            "dewpoint_avg",
            "dewpoint_min",
            "dewpoint_max",
            "temperatura_avg",
            "temperatura_min",
            "temperatura_max",
            "umidita_relativa_avg",
            "cons_specifico_avg",
        },
        "1d": {
            "idSala",
            "bucket_start",
            "samples_count",
            "volume_nm3_sum",
            "energia_kwh_sum",
            "pressione_avg",
            "pressione_min",
            "pressione_max",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "flusso_nm3h_avg",
            "flusso_nm3h_min",
            "flusso_nm3h_max",
            "dewpoint_avg",
            "dewpoint_min",
            "dewpoint_max",
            "temperatura_avg",
            "temperatura_min",
            "temperatura_max",
            "umidita_relativa_avg",
            "cons_specifico_avg",
        },
        "1month": {
            "idSala",
            "bucket_start",
            "volume_nm3_sum",
            "energia_kwh_sum",
            "pressione_avg",
            "potenza_kw_avg",
            "flusso_nm3h_avg",
            "dewpoint_avg",
            "temperatura_avg",
        },
    },
    "compressori": {
        "1min": {
            "idCompressore",
            "idSala",
            "bucket_start",
            "samples_count",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "energia_kwh_sum",
            "u1_avg",
            "u2_avg",
            "u3_avg",
            "i1_avg",
            "i2_avg",
            "i3_avg",
            "cosphi_avg",
            "minuti_on",
            "minuti_standby",
            "minuti_off",
        },
        "1h": {
            "idCompressore",
            "idSala",
            "bucket_start",
            "samples_count",
            "potenza_kw_avg",
            "potenza_kw_min",
            "potenza_kw_max",
            "energia_kwh_sum",
            "u1_avg",
            "u2_avg",
            "u3_avg",
            "i1_avg",
            "i2_avg",
            "i3_avg",
            "cosphi_avg",
            "minuti_on",
            "minuti_standby",
            "minuti_off",
        },
    },
}


def dataset_spec(dataset_name: str) -> AggregateDatasetSpec:
    try:
        return DATASETS[dataset_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported aggregate dataset: {dataset_name!r}") from exc


def dataset_granularities(dataset_name: str) -> tuple[str, ...]:
    spec = dataset_spec(dataset_name)
    return tuple(level.granularity for level in spec.levels)


def level_spec_for_granularity(dataset_name: str, granularity: str) -> AggregateLevelSpec:
    spec = dataset_spec(dataset_name)
    for level in spec.levels:
        if level.granularity == granularity:
            return level
    raise ValueError(f"Unsupported granularity {granularity!r} for dataset {dataset_name!r}")


def source_table_for_granularity(dataset_name: str, granularity: str) -> str:
    return level_spec_for_granularity(dataset_name, granularity).source_table


def target_table_for_granularity(dataset_name: str, granularity: str) -> str:
    return level_spec_for_granularity(dataset_name, granularity).target_table


def load_dataset_tables(engine: Engine, dataset_name: str) -> dict[str, Table]:
    spec = dataset_spec(dataset_name)
    metadata = MetaData()
    table_names = [spec.raw_table]
    table_names.extend(level.target_table for level in spec.levels)
    metadata.reflect(bind=engine, only=table_names)
    missing = [table_name for table_name in table_names if table_name not in metadata.tables]
    if missing:
        raise RuntimeError(f"Missing required aggregate tables for dataset {dataset_name}: {', '.join(missing)}")
    return {table_name: metadata.tables[table_name] for table_name in table_names}


def validate_rollup_schema(engine: Engine) -> list[str]:
    inspector = inspect(engine)
    warnings: list[str] = []

    for dataset_name, per_granularity in ROLLUP_REQUIRED_COLUMNS.items():
        for granularity, required_columns in per_granularity.items():
            table_name = target_table_for_granularity(dataset_name, granularity)
            actual_columns = {column["name"] for column in inspector.get_columns(table_name)}
            missing = sorted(required_columns.difference(actual_columns))
            if missing:
                raise RuntimeError(
                    f"Aggregate table {table_name} is missing required columns for pyramid rollups: {', '.join(missing)}"
                )

    monthly_columns = {column["name"] for column in inspector.get_columns("sale_agg_1month")}
    optional_monthly_columns = {"samples_count", "cons_specifico_avg", "umidita_relativa_avg"}
    missing_monthly_optional = sorted(optional_monthly_columns.difference(monthly_columns))
    if missing_monthly_optional:
        warnings.append(
            "sale_agg_1month does not persist optional terminal rollup columns: "
            + ", ".join(missing_monthly_optional)
        )

    return warnings


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def granularity_step(granularity: str) -> timedelta | None:
    if granularity == "1min":
        return timedelta(minutes=1)
    if granularity == "15min":
        return timedelta(minutes=15)
    if granularity == "1h":
        return timedelta(hours=1)
    if granularity == "1d":
        return timedelta(days=1)
    if granularity == "1month":
        return None
    raise ValueError(f"Unsupported granularity {granularity!r}")


def floor_datetime_to_granularity(value: datetime, granularity: str) -> datetime:
    if granularity == "1month":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if granularity == "1d":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if granularity == "1h":
        return value.replace(minute=0, second=0, microsecond=0)
    if granularity == "15min":
        minute = (value.minute // 15) * 15
        return value.replace(minute=minute, second=0, microsecond=0)
    if granularity == "1min":
        return value.replace(second=0, microsecond=0)
    raise ValueError(f"Unsupported granularity {granularity!r}")


def ceil_datetime_to_granularity(value: datetime, granularity: str) -> datetime:
    floored = floor_datetime_to_granularity(value, granularity)
    if value == floored:
        return value
    if granularity == "1month":
        return add_months(floored, 1)
    step = granularity_step(granularity)
    assert step is not None
    return floored + step


def expand_range_for_granularity(range_start: datetime, range_end: datetime, granularity: str) -> tuple[datetime, datetime]:
    if range_start >= range_end:
        raise ValueError("Invalid range for aggregate refresh: start must be lower than end.")
    return (
        floor_datetime_to_granularity(range_start, granularity),
        ceil_datetime_to_granularity(range_end, granularity),
    )


def bucket_expression_sql(granularity: str, column_sql: str) -> str:
    if granularity == "1month":
        return f"STR_TO_DATE(DATE_FORMAT({column_sql}, '%Y-%m-01 00:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "1d":
        return f"STR_TO_DATE(DATE_FORMAT({column_sql}, '%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "1h":
        return f"STR_TO_DATE(DATE_FORMAT({column_sql}, '%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%i:%s')"
    if granularity == "15min":
        return f"FROM_UNIXTIME(UNIX_TIMESTAMP({column_sql}) - MOD(UNIX_TIMESTAMP({column_sql}), 900))"
    if granularity == "1min":
        return f"FROM_UNIXTIME(UNIX_TIMESTAMP({column_sql}) - MOD(UNIX_TIMESTAMP({column_sql}), 60))"
    raise ValueError(f"Unsupported granularity {granularity!r}")


def bucket_end_expression_sql(granularity: str, bucket_sql: str) -> str:
    if granularity == "1month":
        return f"DATE_ADD({bucket_sql}, INTERVAL 1 MONTH)"
    if granularity == "1d":
        return f"DATE_ADD({bucket_sql}, INTERVAL 1 DAY)"
    if granularity == "1h":
        return f"DATE_ADD({bucket_sql}, INTERVAL 1 HOUR)"
    if granularity == "15min":
        return f"DATE_ADD({bucket_sql}, INTERVAL 15 MINUTE)"
    if granularity == "1min":
        return f"DATE_ADD({bucket_sql}, INTERVAL 1 MINUTE)"
    raise ValueError(f"Unsupported granularity {granularity!r}")


def bucket_duration_hours_sql(granularity: str, bucket_sql: str) -> str:
    return f"(TIMESTAMPDIFF(SECOND, {bucket_sql}, {bucket_end_expression_sql(granularity, bucket_sql)}) / 3600.0)"


def build_filter_sql(filter_column: str, entity_ids: list[int] | None, alias: str = "src") -> tuple[str, dict[str, int]]:
    if not entity_ids:
        return "", {}
    placeholders = ", ".join(f":filter_id_{index}" for index, _ in enumerate(entity_ids))
    params = {f"filter_id_{index}": entity_id for index, entity_id in enumerate(entity_ids)}
    return f" AND {alias}.`{filter_column}` IN ({placeholders})", params


def weighted_average_sql(column_name: str, samples_column: str = "samples_count") -> str:
    weight_sql = f"CASE WHEN src.`{column_name}` IS NOT NULL AND src.`{samples_column}` IS NOT NULL THEN src.`{samples_column}` ELSE 0 END"
    numerator_sql = f"SUM(CASE WHEN src.`{column_name}` IS NOT NULL AND src.`{samples_column}` IS NOT NULL THEN src.`{column_name}` * src.`{samples_column}` ELSE 0 END)"
    return f"CASE WHEN SUM({weight_sql}) > 0 THEN {numerator_sql} / NULLIF(SUM({weight_sql}), 0) ELSE NULL END"


def sale_flow_expression(alias: str) -> str:
    return f"COALESCE({alias}.`flusso_tot`, {alias}.`flusso`)"


def sale_dewpoint_expression(alias: str) -> str:
    return f"COALESCE({alias}.`dewpoint`, {alias}.`dewpoint_td`)"


def compressor_state_case_sql(alias: str) -> str:
    return (
        "CASE "
        f"WHEN COALESCE({alias}.`potAttiva`, 0) > 0.5 "
        f"OR COALESCE({alias}.`l1`, 0) > 1 "
        f"OR COALESCE({alias}.`l2`, 0) > 1 "
        f"OR COALESCE({alias}.`l3`, 0) > 1 "
        "THEN 'ON' "
        f"WHEN COALESCE({alias}.`u1`, 0) > 10 "
        f"OR COALESCE({alias}.`u2`, 0) > 10 "
        f"OR COALESCE({alias}.`u3`, 0) > 10 "
        "THEN 'STANDBY' "
        "ELSE 'OFF' END"
    )


def build_sale_raw_select_expressions(
    target_columns: set[str],
    granularity: str,
    bucket_alias: str = "rollup_bucket_start",
) -> dict[str, str]:
    bucket_sql = bucket_alias
    duration_hours_sql = bucket_duration_hours_sql(granularity, bucket_sql)
    volume_sql = "GREATEST(COALESCE(MAX(src.`totMetriCubi`) - MIN(src.`totMetriCubi`), 0), 0)"
    energy_sql = f"(AVG(src.`potAttTotale`) * {duration_hours_sql})"
    flow_sql = sale_flow_expression("src")
    dewpoint_sql = sale_dewpoint_expression("src")

    expressions: dict[str, str] = {
        "idSala": "src.`idSala`",
        "bucket_start": bucket_sql,
        "samples_count": "COUNT(*)",
        "volume_nm3_sum": volume_sql,
        "energia_kwh_sum": energy_sql,
        "pressione_avg": "AVG(src.`pressione`)",
        "pressione_min": "MIN(src.`pressione`)",
        "pressione_max": "MAX(src.`pressione`)",
        "potenza_kw_avg": "AVG(src.`potAttTotale`)",
        "potenza_kw_min": "MIN(src.`potAttTotale`)",
        "potenza_kw_max": "MAX(src.`potAttTotale`)",
        "flusso_nm3h_avg": f"AVG({flow_sql})",
        "flusso_nm3h_min": f"MIN({flow_sql})",
        "flusso_nm3h_max": f"MAX({flow_sql})",
        "dewpoint_avg": f"AVG({dewpoint_sql})",
        "dewpoint_min": f"MIN({dewpoint_sql})",
        "dewpoint_max": f"MAX({dewpoint_sql})",
        "temperatura_avg": "AVG(src.`temperatura`)",
        "temperatura_min": "MIN(src.`temperatura`)",
        "temperatura_max": "MAX(src.`temperatura`)",
        "umidita_relativa_avg": "AVG(src.`umidita_relativa`)",
        "cons_specifico_avg": (
            f"CASE WHEN {volume_sql} > 0 THEN {energy_sql} / NULLIF({volume_sql}, 0) "
            "ELSE AVG(src.`consSpecifico`) END"
        ),
        "updated_at": "CURRENT_TIMESTAMP",
    }
    return {column_name: expression for column_name, expression in expressions.items() if column_name in target_columns}


def build_sale_rollup_select_expressions(target_columns: set[str]) -> dict[str, str]:
    sum_volume_sql = "SUM(COALESCE(src.`volume_nm3_sum`, 0))"
    sum_energy_sql = "SUM(COALESCE(src.`energia_kwh_sum`, 0))"

    expressions: dict[str, str] = {
        "idSala": "src.`idSala`",
        "bucket_start": "bucket_start",
        "samples_count": "SUM(COALESCE(src.`samples_count`, 0))",
        "volume_nm3_sum": sum_volume_sql,
        "energia_kwh_sum": sum_energy_sql,
        "pressione_avg": weighted_average_sql("pressione_avg"),
        "pressione_min": "MIN(src.`pressione_min`)",
        "pressione_max": "MAX(src.`pressione_max`)",
        "potenza_kw_avg": weighted_average_sql("potenza_kw_avg"),
        "potenza_kw_min": "MIN(src.`potenza_kw_min`)",
        "potenza_kw_max": "MAX(src.`potenza_kw_max`)",
        "flusso_nm3h_avg": weighted_average_sql("flusso_nm3h_avg"),
        "flusso_nm3h_min": "MIN(src.`flusso_nm3h_min`)",
        "flusso_nm3h_max": "MAX(src.`flusso_nm3h_max`)",
        "dewpoint_avg": weighted_average_sql("dewpoint_avg"),
        "dewpoint_min": "MIN(src.`dewpoint_min`)",
        "dewpoint_max": "MAX(src.`dewpoint_max`)",
        "temperatura_avg": weighted_average_sql("temperatura_avg"),
        "temperatura_min": "MIN(src.`temperatura_min`)",
        "temperatura_max": "MAX(src.`temperatura_max`)",
        "umidita_relativa_avg": weighted_average_sql("umidita_relativa_avg"),
        "cons_specifico_avg": (
            f"CASE WHEN {sum_volume_sql} > 0 THEN {sum_energy_sql} / NULLIF({sum_volume_sql}, 0) ELSE NULL END"
        ),
        "updated_at": "CURRENT_TIMESTAMP",
    }
    return {column_name: expression for column_name, expression in expressions.items() if column_name in target_columns}


def build_compressori_raw_select_expressions(
    target_columns: set[str],
    bucket_alias: str = "rollup_bucket_start",
) -> dict[str, str]:
    bucket_sql = bucket_alias
    duration_hours_sql = bucket_duration_hours_sql("1min", bucket_sql)
    state_case_sql = compressor_state_case_sql("src")
    on_ratio_sql = f"(SUM(CASE WHEN {state_case_sql} = 'ON' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0))"
    standby_ratio_sql = f"(SUM(CASE WHEN {state_case_sql} = 'STANDBY' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0))"
    off_ratio_sql = f"(SUM(CASE WHEN {state_case_sql} = 'OFF' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0))"

    expressions: dict[str, str] = {
        "idCompressore": "src.`idCompressore`",
        "idSala": "src.`idSala`",
        "bucket_start": bucket_sql,
        "samples_count": "COUNT(*)",
        "potenza_kw_avg": "AVG(src.`potAttiva`)",
        "potenza_kw_min": "MIN(src.`potAttiva`)",
        "potenza_kw_max": "MAX(src.`potAttiva`)",
        "energia_kwh_sum": f"(AVG(src.`potAttiva`) * {duration_hours_sql})",
        "u1_avg": "AVG(src.`u1`)",
        "u2_avg": "AVG(src.`u2`)",
        "u3_avg": "AVG(src.`u3`)",
        "i1_avg": "AVG(src.`l1`)",
        "i2_avg": "AVG(src.`l2`)",
        "i3_avg": "AVG(src.`l3`)",
        "cosphi_avg": "AVG(src.`cosphi`)",
        "minuti_on": on_ratio_sql,
        "minuti_standby": standby_ratio_sql,
        "minuti_off": off_ratio_sql,
        "last_stato_compressore": (
            f"SUBSTRING_INDEX(GROUP_CONCAT({state_case_sql} ORDER BY src.`timestamp` DESC SEPARATOR ','), ',', 1)"
        ),
        "stato_dati_last": (
            "SUBSTRING_INDEX(GROUP_CONCAT(CAST(src.`statoDati` AS CHAR) ORDER BY src.`timestamp` DESC SEPARATOR ','), ',', 1)"
        ),
        "updated_at": "CURRENT_TIMESTAMP",
    }
    return {column_name: expression for column_name, expression in expressions.items() if column_name in target_columns}


def build_compressori_rollup_select_expressions(target_columns: set[str]) -> dict[str, str]:
    expressions: dict[str, str] = {
        "idCompressore": "src.`idCompressore`",
        "idSala": "src.`idSala`",
        "bucket_start": "bucket_start",
        "samples_count": "SUM(COALESCE(src.`samples_count`, 0))",
        "potenza_kw_avg": weighted_average_sql("potenza_kw_avg"),
        "potenza_kw_min": "MIN(src.`potenza_kw_min`)",
        "potenza_kw_max": "MAX(src.`potenza_kw_max`)",
        "energia_kwh_sum": "SUM(COALESCE(src.`energia_kwh_sum`, 0))",
        "u1_avg": weighted_average_sql("u1_avg"),
        "u2_avg": weighted_average_sql("u2_avg"),
        "u3_avg": weighted_average_sql("u3_avg"),
        "i1_avg": weighted_average_sql("i1_avg"),
        "i2_avg": weighted_average_sql("i2_avg"),
        "i3_avg": weighted_average_sql("i3_avg"),
        "cosphi_avg": weighted_average_sql("cosphi_avg"),
        "minuti_on": "SUM(COALESCE(src.`minuti_on`, 0))",
        "minuti_standby": "SUM(COALESCE(src.`minuti_standby`, 0))",
        "minuti_off": "SUM(COALESCE(src.`minuti_off`, 0))",
        "updated_at": "CURRENT_TIMESTAMP",
    }
    return {column_name: expression for column_name, expression in expressions.items() if column_name in target_columns}


def build_select_expressions(
    dataset_name: str,
    level: AggregateLevelSpec,
    target_table: Table,
    bucket_alias: str = "rollup_bucket_start",
) -> dict[str, str]:
    target_columns = set(target_table.columns.keys())
    if dataset_name == "sale" and level.source_kind == "raw":
        return build_sale_raw_select_expressions(target_columns, level.granularity, bucket_alias)
    if dataset_name == "sale":
        return build_sale_rollup_select_expressions(target_columns)
    if dataset_name == "compressori" and level.source_kind == "raw":
        return build_compressori_raw_select_expressions(target_columns, bucket_alias)
    if dataset_name == "compressori":
        return build_compressori_rollup_select_expressions(target_columns)
    raise ValueError(f"Unsupported dataset {dataset_name!r}")


def ensure_group_columns_present(target_table: Table, group_columns: tuple[str, ...]) -> None:
    missing = [column_name for column_name in group_columns if column_name not in target_table.columns]
    if missing:
        raise RuntimeError(
            f"Target table {target_table.name} is missing required grouping columns for pyramid rollups: {', '.join(missing)}"
        )


def build_update_columns(target_table: Table, insert_columns: tuple[str, ...], group_columns: tuple[str, ...]) -> tuple[str, ...]:
    immutable = set(group_columns) | {"bucket_start"}
    return tuple(
        column_name
        for column_name in insert_columns
        if column_name not in immutable
    )


def build_delete_sql(dataset_name: str, granularity: str, entity_ids: list[int] | None = None) -> tuple[str, dict[str, Any]]:
    spec = dataset_spec(dataset_name)
    level = level_spec_for_granularity(dataset_name, granularity)
    filter_sql, filter_params = build_filter_sql(spec.filter_column, entity_ids, alias="target")
    sql = (
        f"DELETE FROM `{level.target_table}` AS target "
        "WHERE target.`bucket_start` >= :range_start "
        "AND target.`bucket_start` < :range_end"
        f"{filter_sql}"
    )
    return sql, {"range_start": None, "range_end": None, **filter_params}


def build_raw_insert_sql(
    dataset_name: str,
    granularity: str,
    tables: dict[str, Table],
    entity_ids: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    spec = dataset_spec(dataset_name)
    level = level_spec_for_granularity(dataset_name, granularity)
    if level.source_kind != "raw":
        raise ValueError(f"Granularity {granularity!r} for dataset {dataset_name!r} is not a raw level.")

    target_table = tables[level.target_table]
    ensure_group_columns_present(target_table, spec.group_columns)
    select_expressions = build_select_expressions(dataset_name, level, target_table)
    select_expressions["bucket_start"] = "rollup_bucket_start"
    insert_columns = tuple(column_name for column_name in target_table.columns.keys() if column_name in select_expressions)
    update_columns = build_update_columns(target_table, insert_columns, spec.group_columns)

    filter_sql, filter_params = build_filter_sql(spec.filter_column, entity_ids, alias="src")
    bucket_sql = bucket_expression_sql(granularity, f"src.`{level.source_time_column}`")

    insert_columns_sql = ", ".join(f"`{column_name}`" for column_name in insert_columns)
    select_columns_sql = ",\n       ".join(
        f"{select_expressions[column_name]} AS `{column_name}`" for column_name in insert_columns
    )
    group_by_sql = ", ".join(f"src.`{column_name}`" for column_name in spec.group_columns)
    update_sql = ",\n    ".join(f"`{column_name}` = VALUES(`{column_name}`)" for column_name in update_columns)

    sql = f"""
INSERT INTO `{level.target_table}` (
    {insert_columns_sql}
)
SELECT
       {select_columns_sql}
FROM (
    SELECT
        src.*,
        {bucket_sql} AS rollup_bucket_start
    FROM `{level.source_table}` AS src
    WHERE src.`{level.source_time_column}` >= :range_start
      AND src.`{level.source_time_column}` < :range_end{filter_sql}
) AS src
GROUP BY {group_by_sql}, rollup_bucket_start
"""
    if update_sql:
        sql = f"{sql}ON DUPLICATE KEY UPDATE\n    {update_sql}"
    return sql.strip(), {"range_start": None, "range_end": None, **filter_params}


def build_rollup_insert_sql(
    dataset_name: str,
    granularity: str,
    tables: dict[str, Table],
    entity_ids: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    spec = dataset_spec(dataset_name)
    level = level_spec_for_granularity(dataset_name, granularity)
    if level.source_kind != "rollup":
        raise ValueError(f"Granularity {granularity!r} for dataset {dataset_name!r} is not a rollup level.")

    target_table = tables[level.target_table]
    ensure_group_columns_present(target_table, spec.group_columns)
    select_expressions = build_select_expressions(dataset_name, level, target_table)
    select_expressions["bucket_start"] = "rollup_bucket_start"
    insert_columns = tuple(column_name for column_name in target_table.columns.keys() if column_name in select_expressions)
    update_columns = build_update_columns(target_table, insert_columns, spec.group_columns)
    filter_sql, filter_params = build_filter_sql(spec.filter_column, entity_ids, alias="src")
    bucket_sql = bucket_expression_sql(granularity, f"src.`{level.source_time_column}`")

    insert_columns_sql = ", ".join(f"`{column_name}`" for column_name in insert_columns)
    select_columns_sql = ",\n       ".join(
        f"{select_expressions[column_name]} AS `{column_name}`" for column_name in insert_columns
    )
    group_by_sql = ", ".join(f"src.`{column_name}`" for column_name in spec.group_columns)
    update_sql = ",\n    ".join(f"`{column_name}` = VALUES(`{column_name}`)" for column_name in update_columns)

    sql = f"""
INSERT INTO `{level.target_table}` (
    {insert_columns_sql}
)
SELECT
       {select_columns_sql}
FROM (
    SELECT
        src.*,
        {bucket_sql} AS rollup_bucket_start
    FROM `{level.source_table}` AS src
    WHERE src.`{level.source_time_column}` >= :range_start
      AND src.`{level.source_time_column}` < :range_end{filter_sql}
) AS src
GROUP BY {group_by_sql}, rollup_bucket_start
"""
    if update_sql:
        sql = f"{sql}ON DUPLICATE KEY UPDATE\n    {update_sql}"
    return sql.strip(), {"range_start": None, "range_end": None, **filter_params}


def build_insert_sql(
    dataset_name: str,
    granularity: str,
    tables: dict[str, Table],
    entity_ids: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    level = level_spec_for_granularity(dataset_name, granularity)
    if level.source_kind == "raw":
        return build_raw_insert_sql(dataset_name, granularity, tables, entity_ids)
    return build_rollup_insert_sql(dataset_name, granularity, tables, entity_ids)


def refresh_aggregate_level(
    connection: Connection,
    dataset_name: str,
    granularity: str,
    requested_start: datetime,
    requested_end: datetime,
    *,
    entity_ids: list[int] | None = None,
    truncate_target_range: bool = True,
    dry_run: bool = False,
    tables: dict[str, Table] | None = None,
) -> RefreshLevelResult:
    spec = dataset_spec(dataset_name)
    level = level_spec_for_granularity(dataset_name, granularity)
    expanded_start, expanded_end = expand_range_for_granularity(requested_start, requested_end, granularity)
    if tables is None:
        tables = load_dataset_tables(connection.engine, dataset_name)

    delete_sql, delete_template = build_delete_sql(dataset_name, granularity, entity_ids)
    insert_sql, insert_template = build_insert_sql(dataset_name, granularity, tables, entity_ids)
    params = {"range_start": expanded_start, "range_end": expanded_end}
    delete_params = {**delete_template, **params}
    insert_params = {**insert_template, **params}

    started_at = perf_counter()
    deleted_rows = 0
    affected_rows = 0

    if not dry_run:
        if truncate_target_range:
            deleted_rows = connection.execute(text(delete_sql), delete_params).rowcount or 0
        affected_rows = connection.execute(text(insert_sql), insert_params).rowcount or 0

    return RefreshLevelResult(
        dataset=spec.name,
        granularity=granularity,
        source_table=level.source_table,
        target_table=level.target_table,
        requested_start=requested_start,
        requested_end=requested_end,
        expanded_start=expanded_start,
        expanded_end=expanded_end,
        deleted_rows=deleted_rows,
        affected_rows=affected_rows,
        elapsed_seconds=perf_counter() - started_at,
    )


def refresh_pyramid_range(
    engine: Engine,
    dataset_name: str,
    requested_start: datetime,
    requested_end: datetime,
    *,
    entity_ids: list[int] | None = None,
    stop_at_granularity: str | None = None,
    truncate_target_range: bool = True,
    dry_run: bool = False,
) -> list[RefreshLevelResult]:
    spec = dataset_spec(dataset_name)
    tables = load_dataset_tables(engine, dataset_name)
    levels = list(spec.levels)
    if stop_at_granularity is not None:
        stop_index = next(
            (index for index, level in enumerate(levels) if level.granularity == stop_at_granularity),
            None,
        )
        if stop_index is None:
            raise ValueError(f"Unsupported stop granularity {stop_at_granularity!r} for dataset {dataset_name!r}")
        levels = levels[: stop_index + 1]

    results: list[RefreshLevelResult] = []
    next_start = requested_start
    next_end = requested_end

    for level in levels:
        with engine.begin() as connection:
            result = refresh_aggregate_level(
                connection,
                dataset_name,
                level.granularity,
                next_start,
                next_end,
                entity_ids=entity_ids,
                truncate_target_range=truncate_target_range,
                dry_run=dry_run,
                tables=tables,
            )
        results.append(result)
        next_start = result.expanded_start
        next_end = result.expanded_end

    return results


def log_refresh_results(results: list[RefreshLevelResult], logger: logging.Logger | None = None) -> None:
    target_logger = logger or LOGGER
    for result in results:
        target_logger.info(
            "aggregate_refresh dataset=%s granularity=%s source=%s target=%s requested=%s->%s expanded=%s->%s deleted=%s affected=%s elapsed=%.2fs",
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


def refresh_pyramid_range_safely(
    engine: Engine,
    dataset_name: str,
    requested_start: datetime,
    requested_end: datetime,
    *,
    entity_ids: list[int] | None = None,
    stop_at_granularity: str | None = None,
    truncate_target_range: bool = True,
    dry_run: bool = False,
    allow_upsert_fallback: bool = False,
    logger: logging.Logger | None = None,
) -> list[RefreshLevelResult]:
    try:
        results = refresh_pyramid_range(
            engine,
            dataset_name,
            requested_start,
            requested_end,
            entity_ids=entity_ids,
            stop_at_granularity=stop_at_granularity,
            truncate_target_range=truncate_target_range,
            dry_run=dry_run,
        )
        log_refresh_results(results, logger)
        return results
    except SQLAlchemyError as exc:
        if truncate_target_range and allow_upsert_fallback and "delete command denied" in str(exc).lower():
            (logger or LOGGER).warning(
                "aggregate_refresh_delete_denied dataset=%s range=%s->%s entity_ids=%s. Retrying without target delete.",
                dataset_name,
                requested_start,
                requested_end,
                entity_ids or "ALL",
            )
            results = refresh_pyramid_range(
                engine,
                dataset_name,
                requested_start,
                requested_end,
                entity_ids=entity_ids,
                stop_at_granularity=stop_at_granularity,
                truncate_target_range=False,
                dry_run=dry_run,
            )
            log_refresh_results(results, logger)
            return results
        (logger or LOGGER).exception(
            "aggregate_refresh_failed dataset=%s range=%s->%s entity_ids=%s",
            dataset_name,
            requested_start,
            requested_end,
            entity_ids or "ALL",
        )
        raise
