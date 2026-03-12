from __future__ import annotations

from collections.abc import Mapping
import logging
import os
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from time import perf_counter
from typing import Any, Iterator

from sqlalchemy import MetaData, and_, case, create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from ..config import settings
from ..schemas import AlarmEvent
from .aggregate_policy import (
    choose_sale_granularity_for_request,
    granularity_span,
    iter_sale_granularity_candidates,
)
from ..scripts.csv_to_db_ingestor import (
    AppConfig as IngestorConfig,
    CsvToDbIngestor,
    DEFAULT_STATUS_FILE,
    PLANT_SEEDS,
    SALA_SEEDS,
    SchemaBundle,
    normalize_lookup_key,
    resolve_database_url,
)

LOGGER = logging.getLogger("energysaving_runtime")

DASHBOARD_REFRESH_SECONDS = 15
DEFAULT_DASHBOARD_POLL_LOOKBACK_DAYS = max(1, int(os.getenv("DASHBOARD_POLL_LOOKBACK_DAYS", "1")))
PERF_DEBUG_ENABLED = os.getenv("SCADA_PERF_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
PERF_WARN_MS = max(1, int(os.getenv("SCADA_PERF_WARN_MS", "300")))

ROOM_SIGNAL_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Pressione", "pressione", "bar"),
    ("Pressione 2", "pressione2", "bar"),
    ("Temperatura", "temperatura", "C"),
    ("Temperatura 2", "temperatura2", "C"),
    ("Dew Point", "dewpoint", "C"),
    ("Dew Point TD", "dewpoint_td", "C"),
    ("Umidita Relativa", "umidita_relativa", "%"),
    ("Flusso", "flusso", "m^3/h"),
    ("Flusso TOT", "flusso_tot", "m^3/h"),
    ("Flusso 7 barg", "flusso_7barg", "m^3/h"),
    ("Potenza Attiva TOT", "potAttTotale", "kW"),
    ("Consumo Specifico", "consSpecifico", "kWh/m^3"),
    ("Totale", "totMetriCubi", "m3"),
    ("Stato Dati", "statoDati", ""),
)

COMPRESSOR_SIGNAL_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Potenza Attiva", "potAttiva", "kW"),
    ("U1", "u1", "V"),
    ("U2", "u2", "V"),
    ("U3", "u3", "V"),
    ("I1", "l1", "A"),
    ("I2", "l2", "A"),
    ("I3", "l3", "A"),
    ("cosphi", "cosphi", ""),
    ("Energia attiva totale", "energiaAttivaTotale", "kWh"),
    ("Stato Compressore", "statoCompressore", ""),
)

ROOM_SIGNAL_BY_COLUMN = {column: (name, unit) for name, column, unit in ROOM_SIGNAL_SPECS}
ROOM_COLUMN_BY_SIGNAL = {normalize_lookup_key(name): column for name, column, _ in ROOM_SIGNAL_SPECS}
ROOM_NON_SIGNAL_COLUMNS = {"idSala", "timestamp", "updated_at"}

ADDITIVE_ROOM_COLUMNS = {
    "flusso",
    "flusso_tot",
    "flusso_7barg",
    "potAttTotale",
    "totMetriCubi",
}

TARGET_ALIAS_TO_SALA_CODE = {
    normalize_lookup_key("LAMINATO"): "LAMINATI",
    normalize_lookup_key("PRIMO ALTA"): "PRIMO_ALTA",
    normalize_lookup_key("PRIMO BASSA"): "PRIMO_BASSA",
    normalize_lookup_key("SS1 COMPOSIZIONE"): "SS1_COMP",
    normalize_lookup_key("SS2 COMPOSIZIONE"): "SS2_COMP",
    normalize_lookup_key("Laminati Bassa"): "LAM_MP_7BAR",
    normalize_lookup_key("Laminati Alta"): "LAM_ALTA",
    normalize_lookup_key("Forno"): "FORNO_EF",
    normalize_lookup_key("Materie Prime"): "COMP_BP",
}

PLANT_ALIAS_TO_IMPIANTO = {
    normalize_lookup_key(seed.nome): seed.id_impianto
    for seed in PLANT_SEEDS
}
PLANT_NAME_BY_ID = {seed.id_impianto: seed.nome for seed in PLANT_SEEDS}


@dataclass(frozen=True)
class AggregateSignalSpec:
    additive: bool
    avg_column: str | None = None
    min_column: str | None = None
    max_column: str | None = None
    sum_column: str | None = None

AGGREGATE_TABLE_BY_GRANULARITY = {
    "1month": "sale_agg_1month",
    "1d": "sale_agg_1d",
    "1h": "sale_agg_1h",
    "15min": "sale_agg_15min",
    "1min": "sale_agg_1min",
}

BUCKET_TO_GRANULARITY = {
    normalize_lookup_key("1 month"): "1month",
    normalize_lookup_key("1month"): "1month",
    normalize_lookup_key("1 day"): "1d",
    normalize_lookup_key("1d"): "1d",
    normalize_lookup_key("1 hour"): "1h",
    normalize_lookup_key("1h"): "1h",
    normalize_lookup_key("15 min"): "15min",
    normalize_lookup_key("15min"): "15min",
    normalize_lookup_key("1 min"): "1min",
    normalize_lookup_key("1min"): "1min",
}

AGGREGATE_SIGNAL_SPECS: tuple[tuple[tuple[str, ...], AggregateSignalSpec], ...] = (
    (
        ("flusso tot", "flusso", "flow"),
        AggregateSignalSpec(
            additive=True,
            avg_column="flusso_nm3h_avg",
            min_column="flusso_nm3h_min",
            max_column="flusso_nm3h_max",
            sum_column="volume_nm3_sum",
        ),
    ),
    (
        ("potenza attiva tot", "potenza attiva", "power"),
        AggregateSignalSpec(
            additive=True,
            avg_column="potenza_kw_avg",
            min_column="potenza_kw_min",
            max_column="potenza_kw_max",
            sum_column="energia_kwh_sum",
        ),
    ),
    (
        ("pressione", "pressure"),
        AggregateSignalSpec(
            additive=False,
            avg_column="pressione_avg",
            min_column="pressione_min",
            max_column="pressione_max",
        ),
    ),
    (
        ("dew point", "dewpoint", "dew"),
        AggregateSignalSpec(
            additive=False,
            avg_column="dewpoint_avg",
            min_column="dewpoint_min",
            max_column="dewpoint_max",
        ),
    ),
    (
        ("temperatura", "temperature", "temp"),
        AggregateSignalSpec(
            additive=False,
            avg_column="temperatura_avg",
            min_column="temperatura_min",
            max_column="temperatura_max",
        ),
    ),
    (
        ("umidita relativa", "relative humidity", "umidita_relativa"),
        AggregateSignalSpec(
            additive=False,
            avg_column="umidita_relativa_avg",
        ),
    ),
    (
        ("consumo specifico", "cons specifico", "consspecifico"),
        AggregateSignalSpec(
            additive=False,
            avg_column="cons_specifico_avg",
        ),
    ),
    (
        ("totale", "totmetricubi", "metri cubi"),
        AggregateSignalSpec(
            additive=True,
            sum_column="volume_nm3_sum",
        ),
    ),
)

COMPRESSOR_IDENTITY_FIELDS = ("idCompressore", "idSala", "codice", "codifica", "nome", "abilitato")
COMPRESSOR_READING_FIELDS = ("timestamp", "potAttiva", "u1", "u2", "u3", "l1", "l2", "l3", "cosphi", "energiaAttivaTotale", "statoCompressore")


@dataclass(frozen=True)
class ResolvedTarget:
    scope: str
    label: str
    sala_ids: list[int]
    sala_codes: list[str]


def _row_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    return row._mapping


def _log_perf(event: str, started_at: float, **fields: Any) -> None:
    elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
    payload = " ".join(f"{key}={value}" for key, value in fields.items())
    message = f"{event} elapsed_ms={elapsed_ms}"
    if payload:
        message = f"{message} {payload}"
    if elapsed_ms >= PERF_WARN_MS:
        LOGGER.warning(message)
    elif PERF_DEBUG_ENABLED:
        LOGGER.debug(message)


def _to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.replace(tzinfo=None)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _room_metric_columns(rows: list[Any]) -> list[str]:
    seen: set[str] = set()
    columns: list[str] = []
    for row in rows:
        for column in _row_mapping(row).keys():
            if column in ROOM_NON_SIGNAL_COLUMNS or column in seen:
                continue
            seen.add(column)
            columns.append(column)
    return columns


def _looks_additive_room_column(column: str) -> bool:
    if column in ADDITIVE_ROOM_COLUMNS:
        return True
    key = normalize_lookup_key(column.removeprefix("csv_").replace("_", " "))
    return (
        key.startswith("flusso")
        or key.startswith("flow")
        or "potenza attiva" in key
        or "metri cubi" in key
    )


def _infer_room_signal_unit(column: str, signal_name: str) -> str:
    key = normalize_lookup_key(f"{column} {signal_name}")
    if "pression" in key or "pressure" in key or key.endswith(" barg"):
        return "bar"
    if "temperatur" in key or "dew" in key or "rugiad" in key:
        return "C"
    if "umid" in key:
        return "%"
    if "flusso" in key or "flow" in key or "portat" in key or "nm3" in key:
        return "m^3/h"
    if "potenza" in key or key.endswith(" kw") or "power" in key:
        return "kW"
    if "consumo specifico" in key or "cons specifico" in key:
        return "kWh/m^3"
    if "metri cubi" in key or key == "totale":
        return "m3"
    return ""


def _room_signal_label(column: str) -> tuple[str, str]:
    mapped = ROOM_SIGNAL_BY_COLUMN.get(column)
    if mapped is not None:
        return mapped
    base = column.removeprefix("csv_").replace("_", " ").strip()
    signal_name = re.sub(r"\s+", " ", base).title() if base else column
    signal_name = signal_name.replace("Td", "TD")
    return signal_name, _infer_room_signal_unit(column, signal_name)


def _pick_compressor_display_name(row_mapping: Any) -> str:
    for key in ("nome", "codice", "codifica"):
        value = row_mapping.get(key)
        if value:
            return str(value)
    return "Compressore"


@lru_cache(maxsize=1)
def get_energysaving_database_url() -> str:
    return resolve_database_url(None)


@lru_cache(maxsize=1)
def get_energysaving_engine():
    return create_engine(get_energysaving_database_url(), pool_pre_ping=True, future=True)


@lru_cache(maxsize=1)
def get_energysaving_schema() -> SchemaBundle:
    return SchemaBundle(get_energysaving_engine())


@lru_cache(maxsize=1)
def get_room_signal_column_map() -> dict[str, str]:
    schema = get_energysaving_schema()
    rs = schema.registrazioni_sale
    mapping: dict[str, str] = {}
    for column in rs.c.keys():
        if column in ROOM_NON_SIGNAL_COLUMNS:
            continue
        signal_name, _ = _room_signal_label(column)
        mapping.setdefault(normalize_lookup_key(signal_name), column)
        mapping.setdefault(normalize_lookup_key(column), column)
    return mapping


@lru_cache(maxsize=1)
def get_sale_aggregate_tables() -> dict[str, Any]:
    metadata = MetaData()
    metadata.reflect(bind=get_energysaving_engine(), only=list(AGGREGATE_TABLE_BY_GRANULARITY.values()))
    tables: dict[str, Any] = {}
    for granularity, table_name in AGGREGATE_TABLE_BY_GRANULARITY.items():
        table = metadata.tables.get(table_name)
        if table is not None:
            tables[granularity] = table
    return tables


def refresh_energysaving_schema_caches() -> None:
    get_energysaving_schema.cache_clear()
    get_room_signal_column_map.cache_clear()
    get_sale_aggregate_tables.cache_clear()


@lru_cache(maxsize=1)
def get_energysaving_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_energysaving_engine(), autoflush=False, autocommit=False, future=True)


@contextmanager
def energysaving_session() -> Iterator[Session]:
    session = get_energysaving_session_factory()()
    try:
        yield session
    finally:
        session.close()


@lru_cache(maxsize=1)
def get_sale_catalog() -> tuple[dict[str, Any], ...]:
    schema = get_energysaving_schema()
    with energysaving_session() as session:
        rows = session.execute(
            select(
                schema.sale.c.idSala,
                schema.sale.c.idImpianto,
                schema.sale.c.codice,
                schema.sale.c.nome,
                schema.sale.c.abilitato,
            ).order_by(schema.sale.c.idImpianto, schema.sale.c.idSala)
        ).mappings().all()
    return tuple(dict(row) for row in rows)


@lru_cache(maxsize=1)
def get_impianto_catalog() -> dict[int, str]:
    schema = get_energysaving_schema()
    with energysaving_session() as session:
        rows = session.execute(
            select(schema.impianti.c.idImpianto, schema.impianti.c.nome)
        ).mappings().all()
    catalog = {int(row["idImpianto"]): str(row["nome"]) for row in rows}
    if not catalog:
        catalog = dict(PLANT_NAME_BY_ID)
    return catalog


def list_enabled_sale_codes() -> list[str]:
    return [str(row["codice"]) for row in get_sale_catalog() if int(row.get("abilitato") or 0) == 1]


def resolve_target(session: Session, identifier: str) -> ResolvedTarget | None:
    del session
    normalized = normalize_lookup_key(identifier)
    sala_code = TARGET_ALIAS_TO_SALA_CODE.get(normalized)
    if sala_code:
        normalized = normalize_lookup_key(sala_code)

    sale_rows = get_sale_catalog()

    for row in sale_rows:
        if normalize_lookup_key(str(row["codice"])) == normalized or normalize_lookup_key(str(row["nome"])) == normalized:
            return ResolvedTarget(
                scope="sala",
                label=str(row["codice"]),
                sala_ids=[int(row["idSala"])],
                sala_codes=[str(row["codice"])],
            )

    impianto_id = PLANT_ALIAS_TO_IMPIANTO.get(normalized)
    if impianto_id is None:
        for catalog_id, catalog_name in get_impianto_catalog().items():
            if normalize_lookup_key(catalog_name) == normalized:
                impianto_id = catalog_id
                break

    if impianto_id is None:
        return None

    rows = [
        row
        for row in sale_rows
        if int(row["idImpianto"]) == impianto_id and int(row.get("abilitato") or 0) == 1
    ]
    if not rows:
        return None

    return ResolvedTarget(
        scope="impianto",
        label=str(get_impianto_catalog().get(impianto_id, PLANT_NAME_BY_ID.get(impianto_id, str(impianto_id)))),
        sala_ids=[int(row["idSala"]) for row in rows],
        sala_codes=[str(row["codice"]) for row in rows],
    )


def _latest_room_rows(session: Session, target: ResolvedTarget) -> list[Any]:
    schema = get_energysaving_schema()
    rs = schema.stato_sale_corrente
    room_fields = [column for column in rs.c.keys() if column != "updated_at"]
    room_columns = [rs.c[field] for field in room_fields]
    started_at = perf_counter()
    stmt = (
        select(*room_columns)
        .where(rs.c.idSala.in_(target.sala_ids))
        .order_by(rs.c.idSala)
    )
    rows = session.execute(stmt).mappings().all()
    _log_perf("dashboard_latest_room_rows", started_at, scope=target.scope, sale_count=len(target.sala_ids), rows=len(rows))
    return rows


def _latest_compressor_rows(session: Session, target: ResolvedTarget) -> list[Any]:
    schema = get_energysaving_schema()
    c = schema.compressori
    rc = schema.stato_compressori_corrente
    started_at = perf_counter()
    compressor_identity_columns = [c.c[field] for field in COMPRESSOR_IDENTITY_FIELDS if field in c.c]
    reading_columns = [rc.c[field].label(field) for field in COMPRESSOR_READING_FIELDS if field in rc.c]

    latest_stmt = (
        select(*compressor_identity_columns, *reading_columns)
        .select_from(rc.join(c, c.c.idCompressore == rc.c.idCompressore))
        .where(rc.c.idSala.in_(target.sala_ids))
        .order_by(c.c.idSala, c.c.idCompressore)
    )
    if "abilitato" in c.c:
        latest_stmt = latest_stmt.where(c.c.abilitato == 1)
    rows = session.execute(latest_stmt).mappings().all()
    _log_perf(
        "dashboard_latest_compressor_rows",
        started_at,
        scope=target.scope,
        sale_count=len(target.sala_ids),
        compressors=len(rows),
        rows=len(rows),
    )
    return rows


def _aggregate_room_metrics(rows: list[Any]) -> dict[str, float]:
    if not rows:
        return {}

    metric_columns = _room_metric_columns(rows)

    if len(rows) == 1:
        mapping = _row_mapping(rows[0])
        result: dict[str, float] = {}
        for column in metric_columns:
            value = _float_or_none(mapping.get(column))
            if value is not None:
                result[column] = value
        return result

    metrics: dict[str, float] = {}
    for column in metric_columns:
        values = [
            _float_or_none(_row_mapping(row).get(column))
            for row in rows
        ]
        valid = [value for value in values if value is not None]
        if not valid:
            continue
        if _looks_additive_room_column(column):
            metrics[column] = sum(valid)
        else:
            metrics[column] = sum(valid) / len(valid)
    return metrics


def _build_summary_signals(room_rows: list[Any], compressor_rows: list[Any]) -> tuple[dict[str, dict[str, Any]], str | None]:
    signals: dict[str, dict[str, Any]] = {}
    latest_iso: str | None = None
    room_metrics = _aggregate_room_metrics(room_rows)

    for column, value in room_metrics.items():
        signal_name, unit = _room_signal_label(column)
        room_ts = _row_mapping(room_rows[0]).get("timestamp") if len(room_rows) == 1 else None
        if len(room_rows) > 1:
            row_ts_values = [
                _row_mapping(row).get("timestamp")
                for row in room_rows
                if _row_mapping(row).get("timestamp") is not None
            ]
            room_ts = max(row_ts_values) if row_ts_values else None
        ts_iso = _to_utc_iso(room_ts)
        latest_iso = max([item for item in [latest_iso, ts_iso] if item], default=None)
        signals[signal_name] = {"value": value, "unit": unit, "ts": ts_iso}

    for row in compressor_rows:
        mapping = _row_mapping(row)
        display_name = _pick_compressor_display_name(mapping)
        ts_iso = _to_utc_iso(mapping.get("timestamp"))
        latest_iso = max([item for item in [latest_iso, ts_iso] if item], default=None)
        for prefix, column, unit in COMPRESSOR_SIGNAL_SPECS:
            value = _float_or_none(mapping.get(column))
            if value is None:
                continue
            signal_name = f"{prefix} {display_name}"
            signals[signal_name] = {"value": value, "unit": unit, "ts": ts_iso}

    return signals, latest_iso


def _build_compressor_cards(compressor_rows: list[Any]) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for row in compressor_rows:
        mapping = _row_mapping(row)
        code = mapping.get("codice") or mapping.get("codifica") or mapping.get("nome") or mapping.get("idCompressore")
        power = _float_or_none(mapping.get("potAttiva")) or 0.0
        stato = _float_or_none(mapping.get("statoCompressore"))
        currents = [_float_or_none(mapping.get(name)) or 0.0 for name in ("l1", "l2", "l3")]
        running = bool((stato is not None and stato > 0) or power > 0.5 or any(value > 1 for value in currents))
        cards.append(
            {
                "id": str(code),
                "running": running,
                "local": False,
                "fault": False,
            }
        )
    return cards


def fetch_dashboard_summary(identifier: str) -> dict[str, Any] | None:
    started_at = perf_counter()
    request_started_at = datetime.now(UTC).replace(tzinfo=None)
    LOGGER.info("[SUMMARY] sala=%s start=%s", identifier, request_started_at.isoformat())
    with energysaving_session() as session:
        resolve_started_at = perf_counter()
        target = resolve_target(session, identifier)
        resolve_ms = round((perf_counter() - resolve_started_at) * 1000, 2)
        if target is None:
            total_ms = round((perf_counter() - started_at) * 1000, 2)
            LOGGER.info(
                "[SUMMARY] sala=%s resolve_target_ms=%s total_ms=%s found=false",
                identifier,
                resolve_ms,
                total_ms,
            )
            return None

        room_query_started_at = perf_counter()
        room_rows = _latest_room_rows(session, target)
        room_query_ms = round((perf_counter() - room_query_started_at) * 1000, 2)
        compressor_query_started_at = perf_counter()
        compressor_rows = _latest_compressor_rows(session, target)
        compressor_query_ms = round((perf_counter() - compressor_query_started_at) * 1000, 2)
        serialize_started_at = perf_counter()
        signals, latest_iso = _build_summary_signals(room_rows, compressor_rows)
        payload = {
            "plant": target.label,
            "last_update": latest_iso,
            "signals": signals,
            "compressors": _build_compressor_cards(compressor_rows),
            "dryers": [],
            "active_alarms": [],
        }
        serialize_ms = round((perf_counter() - serialize_started_at) * 1000, 2)
        _log_perf(
            "dashboard_summary_build",
            serialize_started_at,
            target=identifier,
            scope=target.scope,
            signals=len(signals),
            compressors=len(compressor_rows),
        )

        LOGGER.debug(
            "dashboard_db_summary target=%s scope=%s sale=%s room_rows=%s compressor_rows=%s latest=%s",
            identifier,
            target.scope,
            target.sala_codes,
            len(room_rows),
            len(compressor_rows),
            latest_iso,
        )
        total_ms = round((perf_counter() - started_at) * 1000, 2)
        LOGGER.info(
            "[SUMMARY] sala=%s scope=%s resolve_target_ms=%s query_stato_sala_ms=%s query_stato_compressori_ms=%s serialize_ms=%s total_ms=%s room_rows=%s compressor_rows=%s latest=%s",
            identifier,
            target.scope,
            resolve_ms,
            room_query_ms,
            compressor_query_ms,
            serialize_ms,
            total_ms,
            len(room_rows),
            len(compressor_rows),
            latest_iso,
        )
        return payload


def fetch_dashboard_alarms(identifier: str) -> list[AlarmEvent]:
    LOGGER.debug("dashboard_db_alarms target=%s count=0", identifier)
    return []


def resolve_room_signal_column(signal: str) -> str | None:
    key = normalize_lookup_key(signal)
    column = get_room_signal_column_map().get(key)
    if column is not None:
        return column
    refresh_energysaving_schema_caches()
    return get_room_signal_column_map().get(key)


def normalize_bucket_granularity(bucket: str | None) -> str | None:
    if not bucket:
        return None
    return BUCKET_TO_GRANULARITY.get(normalize_lookup_key(bucket))


def choose_aggregate_granularity(
    bucket: str | None,
    from_value: datetime | None,
    to_value: datetime | None,
    minutes: int,
) -> str | None:
    explicit = normalize_bucket_granularity(bucket)
    return choose_sale_granularity_for_request(explicit, from_value, to_value, minutes)


def resolve_aggregate_signal_spec(signal: str) -> AggregateSignalSpec | None:
    key = normalize_lookup_key(signal)
    for aliases, spec in AGGREGATE_SIGNAL_SPECS:
        if key in {normalize_lookup_key(alias) for alias in aliases}:
            return spec
    return None


def resolve_aggregate_value_column(table: Any, signal: str, agg: str) -> tuple[Any, AggregateSignalSpec] | None:
    spec = resolve_aggregate_signal_spec(signal)
    if spec is None:
        return None

    requested_column_name = {
        "avg": spec.avg_column,
        "min": spec.min_column,
        "max": spec.max_column,
        "sum": spec.sum_column,
    }.get(agg)

    if requested_column_name and requested_column_name in table.c:
        return table.c[requested_column_name], spec

    if agg == "sum" and spec.additive:
        for fallback_name in (spec.sum_column, spec.avg_column):
            if fallback_name and fallback_name in table.c:
                return table.c[fallback_name], spec
        return None

    for fallback_name in (spec.avg_column, spec.min_column, spec.max_column):
        if fallback_name and fallback_name in table.c:
            return table.c[fallback_name], spec

    return None


def _weighted_group_average_expression(value_column: Any, samples_column: Any) -> Any:
    safe_weight = func.coalesce(samples_column, 0)
    weighted_numerator = func.sum(
        case(
            (value_column.is_not(None), value_column * safe_weight),
            else_=0,
        )
    )
    weighted_denominator = func.sum(
        case(
            (value_column.is_not(None), safe_weight),
            else_=0,
        )
    )
    return case(
        (weighted_denominator > 0, weighted_numerator / func.nullif(weighted_denominator, 0)),
        else_=None,
    )


def _aggregate_group_expression(table: Any, value_column: Any, spec: AggregateSignalSpec, agg: str) -> Any:
    if agg == "sum":
        return func.sum(value_column)
    if agg == "min":
        return func.min(value_column)
    if agg == "max":
        return func.max(value_column)
    if agg != "avg":
        return func.avg(value_column)

    if spec.avg_column == "cons_specifico_avg" and "energia_kwh_sum" in table.c and "volume_nm3_sum" in table.c:
        sum_energy = func.sum(func.coalesce(table.c.energia_kwh_sum, 0))
        sum_volume = func.sum(func.coalesce(table.c.volume_nm3_sum, 0))
        return case(
            (sum_volume > 0, sum_energy / func.nullif(sum_volume, 0)),
            else_=None,
        )

    if "samples_count" in table.c and getattr(value_column, "name", None) == spec.avg_column:
        return _weighted_group_average_expression(value_column, table.c.samples_count)

    return func.avg(value_column)


def _allow_raw_timeseries_fallback(
    bucket: str | None,
    from_value: datetime | None,
    to_value: datetime | None,
    minutes: int,
) -> bool:
    if bucket is not None:
        return False
    if from_value is not None or to_value is not None:
        return False
    return minutes <= 360


def aggregate_granularity_span(granularity: str) -> timedelta:
    return granularity_span(granularity)


def query_aggregate_timeseries(
    identifier: str,
    signal: str,
    from_value: datetime | None,
    to_value: datetime | None,
    minutes: int,
    max_points: int,
    bucket: str | None,
    agg: str,
) -> list[dict[str, Any]] | None:
    preferred_granularity = choose_aggregate_granularity(bucket, from_value, to_value, minutes)
    with energysaving_session() as session:
        target = resolve_target(session, identifier)
        if target is None:
            return None

        for granularity in iter_sale_granularity_candidates(preferred_granularity):
            table = get_sale_aggregate_tables().get(granularity)
            if table is None:
                LOGGER.debug("dashboard_db_timeseries aggregate_table_missing granularity=%s", granularity)
                continue

            resolved = resolve_aggregate_value_column(table, signal, agg)
            if resolved is None:
                LOGGER.debug(
                    "dashboard_db_timeseries aggregate_signal_missing granularity=%s signal=%s agg=%s",
                    granularity,
                    signal,
                    agg,
                )
                continue

            value_column, spec = resolved
            bucket_column = table.c.bucket_start
            span = aggregate_granularity_span(granularity)

            availability_stmt = select(
                func.min(bucket_column).label("available_from"),
                func.max(bucket_column).label("available_to"),
            ).where(and_(table.c.idSala.in_(target.sala_ids), value_column.is_not(None)))
            availability = session.execute(availability_stmt).mappings().first()
            available_from = availability.get("available_from") if availability else None
            available_to = availability.get("available_to") if availability else None
            if available_from is None or available_to is None:
                continue
            if from_value is not None and available_from > from_value:
                LOGGER.debug(
                    "dashboard_db_timeseries aggregate_coverage_start_missing target=%s signal=%s granularity=%s available_from=%s from=%s",
                    identifier,
                    signal,
                    granularity,
                    available_from,
                    from_value,
                )
                continue
            if to_value is not None and available_to + span < to_value:
                LOGGER.debug(
                    "dashboard_db_timeseries aggregate_coverage_end_missing target=%s signal=%s granularity=%s available_to=%s to=%s",
                    identifier,
                    signal,
                    granularity,
                    available_to,
                    to_value,
                )
                continue

            conditions = [table.c.idSala.in_(target.sala_ids), value_column.is_not(None)]
            if from_value is not None:
                conditions.append(bucket_column >= from_value)
            if to_value is not None:
                conditions.append(bucket_column < to_value)

            if len(target.sala_ids) == 1:
                stmt = (
                    select(bucket_column.label("timestamp"), value_column.label("value"))
                    .where(and_(*conditions))
                    .order_by(bucket_column.asc())
                    .limit(max_points)
                )
            else:
                stmt = (
                    select(
                        bucket_column.label("timestamp"),
                        _aggregate_group_expression(table, value_column, spec, agg).label("value"),
                    )
                    .where(and_(*conditions))
                    .group_by(bucket_column)
                    .order_by(bucket_column.asc())
                    .limit(max_points)
                )

            rows = session.execute(stmt).all()
            points = [
                {
                    "ts": _to_utc_iso(row._mapping["timestamp"]),
                    "value": float(row._mapping["value"]),
                }
                for row in rows
                if row._mapping["value"] is not None
            ]
            if not points:
                continue

            LOGGER.debug(
                "dashboard_db_timeseries_aggregate target=%s signal=%s points=%s granularity=%s agg=%s",
                identifier,
                signal,
                len(points),
                granularity,
                agg,
            )
            return points

    LOGGER.debug(
        "dashboard_db_timeseries aggregate_candidates_exhausted target=%s signal=%s preferred=%s agg=%s",
        identifier,
        signal,
        preferred_granularity,
        agg,
    )
    return None


def fetch_dashboard_timeseries(
    identifier: str,
    signal: str,
    from_ts: str | None,
    to_ts: str | None,
    minutes: int,
    max_points: int,
    bucket: str | None,
    agg: str,
) -> list[dict[str, Any]] | None:
    started_at = perf_counter()
    explicit_from_value = _parse_iso_datetime(from_ts)
    explicit_to_value = _parse_iso_datetime(to_ts)
    from_value = _parse_iso_datetime(from_ts)
    to_value = _parse_iso_datetime(to_ts)
    if from_value is None and to_value is None:
        from_value = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=minutes)

    aggregate_points = query_aggregate_timeseries(
        identifier=identifier,
        signal=signal,
        from_value=from_value,
        to_value=to_value,
        minutes=minutes,
        max_points=max_points,
        bucket=bucket,
        agg=agg,
    )
    if aggregate_points is not None:
        _log_perf(
            "dashboard_timeseries_total",
            started_at,
            target=identifier,
            scope="aggregate",
            signal=signal,
            points=len(aggregate_points),
            bucket=bucket or "auto",
        )
        return aggregate_points

    if not _allow_raw_timeseries_fallback(bucket, explicit_from_value, explicit_to_value, minutes):
        LOGGER.debug(
            "dashboard_db_timeseries raw_fallback_blocked target=%s signal=%s bucket=%s from=%s to=%s minutes=%s",
            identifier,
            signal,
            bucket,
            from_value,
            to_value,
            minutes,
        )
        return []

    column = resolve_room_signal_column(signal)
    if column is None:
        LOGGER.debug("dashboard_db_timeseries unsupported_signal target=%s signal=%s", identifier, signal)
        return None

    schema = get_energysaving_schema()
    rs = schema.registrazioni_sale

    with energysaving_session() as session:
        target = resolve_target(session, identifier)
        if target is None:
            return None

        conditions = [rs.c.idSala.in_(target.sala_ids), getattr(rs.c, column).is_not(None)]
        if from_value is not None:
            conditions.append(rs.c.timestamp >= from_value)
        if to_value is not None:
            conditions.append(rs.c.timestamp < to_value)

        if bucket:
            if bucket != "1 month":
                raise ValueError(f"Unsupported bucket: {bucket}")
            bucket_expr = func.date_format(rs.c.timestamp, "%Y-%m-01 00:00:00")
            value_column = getattr(rs.c, column)
            agg_expr = getattr(func, agg)(value_column)
            stmt = (
                select(bucket_expr.label("bucket_ts"), agg_expr.label("value"))
                .where(and_(*conditions))
                .group_by(bucket_expr)
                .order_by(bucket_expr.asc())
                .limit(max_points)
            )
            rows = session.execute(stmt).all()
            points = [
                {
                    "ts": _to_utc_iso(_parse_iso_datetime(row._mapping["bucket_ts"])),
                    "value": float(row._mapping["value"]),
                }
                for row in rows
                if row._mapping["value"] is not None
            ]
        else:
            value_column = getattr(rs.c, column)
            if len(target.sala_ids) == 1:
                stmt = (
                    select(rs.c.timestamp, value_column.label("value"))
                    .where(and_(*conditions))
                    .order_by(rs.c.timestamp.desc())
                    .limit(max_points)
                )
                rows = list(reversed(session.execute(stmt).all()))
                points = [
                    {
                        "ts": _to_utc_iso(row._mapping["timestamp"]),
                        "value": float(row._mapping["value"]),
                    }
                    for row in rows
                    if row._mapping["value"] is not None
                ]
            else:
                aggregate_fn = func.sum if _looks_additive_room_column(column) else func.avg
                stmt = (
                    select(rs.c.timestamp, aggregate_fn(value_column).label("value"))
                    .where(and_(*conditions))
                    .group_by(rs.c.timestamp)
                    .order_by(rs.c.timestamp.desc())
                    .limit(max_points)
                )
                rows = list(reversed(session.execute(stmt).all()))
                points = [
                    {
                        "ts": _to_utc_iso(row._mapping["timestamp"]),
                        "value": float(row._mapping["value"]),
                    }
                    for row in rows
                    if row._mapping["value"] is not None
                ]

        LOGGER.debug(
            "dashboard_db_timeseries target=%s scope=%s signal=%s column=%s points=%s bucket=%s",
            identifier,
            target.scope,
            signal,
            column,
            len(points),
            bucket or "raw",
        )
        _log_perf(
            "dashboard_timeseries_total",
            started_at,
            target=identifier,
            scope=target.scope,
            signal=signal,
            points=len(points),
            bucket=bucket or "raw",
        )
        return points


def fetch_dashboard_monthly_overview(
    identifier: str,
    from_ts: str | None,
    to_ts: str | None,
) -> dict[str, Any] | None:
    started_at = perf_counter()
    from_value = _parse_iso_datetime(from_ts)
    to_value = _parse_iso_datetime(to_ts)
    table = get_sale_aggregate_tables().get("1d")
    if table is None:
        LOGGER.debug("dashboard_monthly_overview aggregate_table_missing granularity=1d")
        return None

    if "volume_nm3_sum" not in table.c or "energia_kwh_sum" not in table.c:
        LOGGER.debug("dashboard_monthly_overview aggregate_columns_missing table=%s", table.name)
        return None

    bucket_column = table.c.bucket_start
    month_expr = func.date_format(bucket_column, "%Y-%m-01 00:00:00")

    with energysaving_session() as session:
        target = resolve_target(session, identifier)
        if target is None:
            return None

        conditions = [table.c.idSala.in_(target.sala_ids)]
        if from_value is not None:
            conditions.append(bucket_column >= from_value)
        if to_value is not None:
            conditions.append(bucket_column < to_value)

        stmt = (
            select(
                month_expr.label("bucket_ts"),
                func.sum(table.c.volume_nm3_sum).label("volume_value"),
                func.sum(table.c.energia_kwh_sum).label("energy_value"),
            )
            .where(and_(*conditions))
            .group_by(month_expr)
            .order_by(month_expr.asc())
        )
        rows = session.execute(stmt).all()

        volume_points: list[dict[str, Any]] = []
        energy_points: list[dict[str, Any]] = []
        for row in rows:
            bucket_ts = _to_utc_iso(_parse_iso_datetime(row._mapping["bucket_ts"]))
            if not bucket_ts:
                continue
            volume_value = row._mapping["volume_value"]
            energy_value = row._mapping["energy_value"]
            if volume_value is not None:
                volume_points.append({"ts": bucket_ts, "value": float(volume_value)})
            if energy_value is not None:
                energy_points.append({"ts": bucket_ts, "value": float(energy_value)})

    from_payload = _to_utc_iso(from_value) or (volume_points[0]["ts"] if volume_points else energy_points[0]["ts"] if energy_points else "")
    to_payload = _to_utc_iso(to_value) or (volume_points[-1]["ts"] if volume_points else energy_points[-1]["ts"] if energy_points else "")
    payload = {
        "plant": target.label if "target" in locals() and target is not None else identifier,
        "source_table": table.name,
        "granularity": "1d",
        "from_ts": from_payload,
        "to_ts": to_payload,
        "range_has_data": bool(volume_points or energy_points),
        "volume_points": volume_points,
        "energy_points": energy_points,
    }
    _log_perf(
        "dashboard_monthly_overview_total",
        started_at,
        target=payload["plant"],
        source_table=table.name,
        volume_points=len(volume_points),
        energy_points=len(energy_points),
    )
    return payload


class CsvDbIngestorService:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._running = False
        self._phase = "idle"
        self._last_error: str | None = None
        self._last_cycle_started_at: datetime | None = None
        self._last_cycle_finished_at: datetime | None = None
        self._ingestor: CsvToDbIngestor | None = None
        self._backfill_remaining_days = 0

    @property
    def is_running(self) -> bool:
        return self._running and self._thread is not None and self._thread.is_alive()

    @property
    def active_jobs_count(self) -> int:
        if self._phase == "poll":
            return 1
        return 0

    def _build_config(self) -> IngestorConfig:
        target_date_raw = os.getenv("SCADA_TARGET_DATE", "").strip()
        from_date_raw = os.getenv("SCADA_FROM_DATE", "").strip()
        to_date_raw = os.getenv("SCADA_TO_DATE", "").strip()
        return IngestorConfig(
            base_csv_url=settings.base_csv_url,
            db_url=get_energysaving_database_url(),
            poll_seconds=max(1, settings.ingest_poll_seconds),
            run_once=False,
            replace_existing=os.getenv("SCADA_REIMPORT_EXISTING", "").strip().lower() in {"1", "true", "yes", "on"},
            status_file=os.getenv("SCADA_STATUS_FILE", DEFAULT_STATUS_FILE).strip() or DEFAULT_STATUS_FILE,
            log_level="INFO",
            target_date=date.fromisoformat(target_date_raw) if target_date_raw else None,
            from_date=date.fromisoformat(from_date_raw) if from_date_raw else None,
            to_date=date.fromisoformat(to_date_raw) if to_date_raw else None,
        )

    def _build_recent_days(self, ingestor: CsvToDbIngestor) -> list[date]:
        today = datetime.now(ingestor.source_tz).date()
        return [today - timedelta(days=offset) for offset in range(DEFAULT_DASHBOARD_POLL_LOOKBACK_DAYS)]

    def _build_backfill_days(self, config: IngestorConfig, ingestor: CsvToDbIngestor) -> list[date]:
        recent_days = set(self._build_recent_days(ingestor))
        if config.from_date or config.to_date:
            start_date = config.from_date or config.to_date
            end_date = config.to_date or config.from_date
            assert start_date is not None
            assert end_date is not None
            return [
                day
                for day in (
                    start_date + timedelta(days=offset)
                    for offset in range((end_date - start_date).days + 1)
                )
                if day not in recent_days
            ]

        initial_day = config.target_date or (datetime.now(ingestor.source_tz).date() - timedelta(days=1))
        if initial_day in recent_days:
            return []
        return [initial_day]

    def _run_cycle_with_metrics(self, mode: str, target_days: list[date]) -> None:
        if not target_days:
            return
        self._last_cycle_started_at = datetime.now(UTC)
        self._ingestor.run_cycle(mode=mode, target_days=target_days)
        self._last_cycle_finished_at = datetime.now(UTC)

    def _run_forever(self) -> None:
        config = self._build_config()
        self._ingestor = CsvToDbIngestor(config)
        recent_days = self._build_recent_days(self._ingestor)
        backlog_days = self._build_backfill_days(config, self._ingestor)
        self._backfill_remaining_days = len(backlog_days)

        try:
            if backlog_days:
                if config.from_date or config.to_date:
                    start_date = config.from_date or config.to_date
                    end_date = config.to_date or config.from_date
                    assert start_date is not None
                    assert end_date is not None
                    LOGGER.info(
                        "dashboard_ingestor_backfill_queued range=%s->%s queued_days=%s",
                        start_date.isoformat(),
                        end_date.isoformat(),
                        len(backlog_days),
                    )
                else:
                    LOGGER.info(
                        "dashboard_ingestor_backfill_queued target_day=%s",
                        backlog_days[0].isoformat(),
                    )
            else:
                LOGGER.info(
                    "dashboard_ingestor_backfill_queued range=none queued_days=0"
                )

            self._phase = "poll"
            LOGGER.info(
                "dashboard_ingestor_poll_ready target_days=%s poll_seconds=%s",
                [day.isoformat() for day in recent_days],
                config.poll_seconds,
            )

            while not self._stop_event.is_set():
                LOGGER.info("dashboard_ingestor_poll_start interval_seconds=%s", config.poll_seconds)
                self._run_cycle_with_metrics(mode="poll", target_days=recent_days)
                LOGGER.info("dashboard_ingestor_poll_complete")

                if backlog_days and not self._stop_event.is_set():
                    next_day = backlog_days.pop(0)
                    self._backfill_remaining_days = len(backlog_days)
                    self._phase = "backfill"
                    LOGGER.info(
                        "dashboard_ingestor_backfill_chunk_start day=%s remaining_after=%s",
                        next_day.isoformat(),
                        len(backlog_days),
                    )
                    self._run_cycle_with_metrics(mode="backfill", target_days=[next_day])
                    LOGGER.info("dashboard_ingestor_backfill_chunk_complete day=%s", next_day.isoformat())
                    self._phase = "poll"
                else:
                    self._backfill_remaining_days = len(backlog_days)

                if self._stop_event.wait(config.poll_seconds):
                    break
        except Exception as exc:
            self._last_error = str(exc)
            LOGGER.exception("dashboard_ingestor_failed")
        finally:
            self._phase = "stopped"
            self._running = False
            self._backfill_remaining_days = 0

    async def start(self) -> bool:
        with self._lock:
            if self.is_running:
                return False
            self._stop_event.clear()
            self._last_error = None
            self._thread = threading.Thread(target=self._run_forever, name="csv-db-ingestor", daemon=True)
            self._running = True
            self._thread.start()
            return True

    async def stop(self) -> bool:
        with self._lock:
            if not self._thread:
                self._running = False
                return False
            self._stop_event.set()
            self._thread.join(timeout=max(DASHBOARD_REFRESH_SECONDS, 5) + 1)
            self._thread = None
            self._running = False
            return True

    def sources(self) -> dict[str, Any]:
        config = self._build_config()
        return {
            "base_csv_url": config.base_csv_url,
            "poll_seconds": config.poll_seconds,
            "running": self.is_running,
            "phase": self._phase,
            "last_error": self._last_error,
            "last_cycle_started_at": _to_utc_iso(self._last_cycle_started_at),
            "last_cycle_finished_at": _to_utc_iso(self._last_cycle_finished_at),
            "backfill_remaining_days": self._backfill_remaining_days,
            "db_url": get_energysaving_database_url(),
        }

    def runtime_jobs(self, plant: str | None = None) -> list[dict[str, Any]]:
        if self._ingestor is None:
            return []

        items: list[dict[str, Any]] = []
        for source_url, state in sorted(self._ingestor.runtime_state.items()):
            if plant and normalize_lookup_key(plant) not in normalize_lookup_key(source_url):
                continue
            items.append(
                {
                    "source_url": source_url,
                    "plant": plant or "",
                    "filename": source_url.rsplit("/", 1)[-1],
                    "last_success_ts": _to_utc_iso(self._last_cycle_finished_at),
                    "last_error": self._last_error,
                    "last_error_ts": _to_utc_iso(self._last_cycle_finished_at) if self._last_error else None,
                    "last_insert_count": None,
                    "last_rows_parsed": None,
                    "last_bytes_read": state.last_bytes_read,
                    "last_bytes_delta": None,
                    "latest_ts_inserted": None,
                    "last_cycle_ts": _to_utc_iso(self._last_cycle_finished_at),
                    "no_progress_cycles": 0,
                    "folder_url": source_url.rsplit("/", 1)[0],
                    "computed_date": None,
                }
            )
        return items


csv_db_ingestor_service = CsvDbIngestorService()


def warm_energysaving_runtime_caches() -> None:
    try:
        get_energysaving_engine()
        get_energysaving_schema()
        get_sale_catalog()
        get_impianto_catalog()
    except Exception:
        LOGGER.exception("energysaving_runtime_warmup_failed")
