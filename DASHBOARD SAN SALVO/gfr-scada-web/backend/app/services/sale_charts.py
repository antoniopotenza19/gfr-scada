from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

from sqlalchemy import MetaData, and_, func, select

from .aggregate_policy import (
    SALE_PRESET_TO_GRANULARITY,
    SALE_REALTIME_PRESETS,
    choose_compressor_activity_granularity,
    choose_sale_granularity_for_window,
    granularity_span,
    iter_sale_granularity_candidates,
)
from .energysaving_runtime import (
    energysaving_session,
    get_energysaving_engine,
    get_energysaving_schema,
    get_impianto_catalog,
    get_sale_catalog,
    resolve_target,
)

SALE_TIMESERIES_TABLES = {
    "1min": "sale_agg_1min",
    "15min": "sale_agg_15min",
    "1h": "sale_agg_1h",
    "1d": "sale_agg_1d",
    "1month": "sale_agg_1month",
}

COMPRESSOR_ACTIVITY_TABLES = {
    "1min": "compressori_agg_1min",
    "1h": "compressori_agg_1h",
}

PRESET_DELTAS = {
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
}

SALE_METRIC_FIELDS = (
    "pressione_avg",
    "potenza_kw_avg",
    "flusso_nm3h_avg",
    "dewpoint_avg",
    "temperatura_avg",
    "cons_specifico_avg",
    "energia_kwh_sum",
    "volume_nm3_sum",
    "samples_count",
)


@dataclass(frozen=True)
class SaleContext:
    sale_id: int
    sale_code: str
    sale_name: str | None
    plant_name: str | None
    last_update: str | None


@dataclass(frozen=True)
class RangeSelection:
    range_key: str | None
    range_start: datetime
    range_end: datetime
    granularity: str
    realtime: bool


def _to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def _to_local_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone().replace(tzinfo=None)
    return value.isoformat(timespec="seconds")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.replace(tzinfo=None)


def _months_ago(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 - months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _years_ago(value: datetime, years: int) -> datetime:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _resolve_preset_window(range_key: str, now_value: datetime) -> tuple[datetime, datetime]:
    if range_key in PRESET_DELTAS:
        return now_value - PRESET_DELTAS[range_key], now_value
    if range_key == "1mo":
        return _months_ago(now_value, 1), now_value
    if range_key == "3mo":
        return _months_ago(now_value, 3), now_value
    if range_key == "6mo":
        return _months_ago(now_value, 6), now_value
    if range_key == "1y":
        return _years_ago(now_value, 1), now_value
    if range_key == "3y":
        return _years_ago(now_value, 3), now_value
    raise ValueError(f"Unsupported range preset: {range_key}")


def resolve_range_selection(
    range_key: str | None,
    from_ts: str | None,
    to_ts: str | None,
) -> RangeSelection:
    now_value = datetime.now(UTC).replace(tzinfo=None)
    normalized_range = (range_key or "").strip().lower() or None
    from_value = _parse_iso_datetime(from_ts)
    to_value = _parse_iso_datetime(to_ts)

    if normalized_range:
        range_start, range_end = _resolve_preset_window(normalized_range, now_value)
        granularity = SALE_PRESET_TO_GRANULARITY[normalized_range]
        realtime = normalized_range in SALE_REALTIME_PRESETS
        return RangeSelection(
            range_key=normalized_range,
            range_start=range_start,
            range_end=range_end,
            granularity=granularity,
            realtime=realtime,
        )

    if from_value is None or to_value is None:
        raise ValueError("Provide either a supported range preset or both from/to timestamps.")
    if from_value >= to_value:
        raise ValueError("Invalid time window: from must be lower than to.")

    return RangeSelection(
        range_key=None,
        range_start=from_value,
        range_end=to_value,
        granularity=choose_sale_granularity_for_window(from_value, to_value),
        realtime=(to_value - from_value) <= timedelta(hours=1),
    )


@lru_cache(maxsize=1)
def get_sale_timeseries_tables() -> dict[str, Any]:
    metadata = MetaData()
    metadata.reflect(bind=get_energysaving_engine(), only=list(SALE_TIMESERIES_TABLES.values()))
    return {
        granularity: metadata.tables[table_name]
        for granularity, table_name in SALE_TIMESERIES_TABLES.items()
        if table_name in metadata.tables
    }


@lru_cache(maxsize=1)
def get_compressor_activity_tables() -> dict[str, Any]:
    metadata = MetaData()
    metadata.reflect(bind=get_energysaving_engine(), only=list(COMPRESSOR_ACTIVITY_TABLES.values()))
    return {
        granularity: metadata.tables[table_name]
        for granularity, table_name in COMPRESSOR_ACTIVITY_TABLES.items()
        if table_name in metadata.tables
    }


def _resolve_sale_context(identifier: str) -> SaleContext:
    schema = get_energysaving_schema()
    sale_catalog = get_sale_catalog()
    impianto_catalog = get_impianto_catalog()

    with energysaving_session() as session:
        sale_row = None
        if identifier.isdigit():
            sale_row = next((row for row in sale_catalog if int(row["idSala"]) == int(identifier)), None)
            if sale_row is None:
                raise ValueError(f"Unknown sale id: {identifier}")
            target_sale_id = int(sale_row["idSala"])
            target_sale_code = str(sale_row["codice"])
        else:
            target = resolve_target(session, identifier)
            if target is None or target.scope != "sala":
                raise ValueError(f"Unknown sale: {identifier}")
            target_sale_id = int(target.sala_ids[0])
            target_sale_code = str(target.sala_codes[0])
            sale_row = next((row for row in sale_catalog if int(row["idSala"]) == target_sale_id), None)

        if sale_row is None:
            raise ValueError(f"Sale not found in catalog: {identifier}")

        last_update = session.execute(
            select(func.max(schema.stato_sale_corrente.c.timestamp)).where(schema.stato_sale_corrente.c.idSala == target_sale_id)
        ).scalar_one_or_none()

    return SaleContext(
        sale_id=target_sale_id,
        sale_code=target_sale_code,
        sale_name=str(sale_row.get("nome") or target_sale_code),
        plant_name=impianto_catalog.get(int(sale_row["idImpianto"])),
        last_update=_to_local_iso(last_update),
    )


def _available_sale_fields(table: Any) -> list[str]:
    return [field for field in SALE_METRIC_FIELDS if field in table.c]


def _query_bucket_availability(table: Any, identifier_column: Any, identifier_value: int) -> tuple[datetime | None, datetime | None]:
    stmt = select(
        func.min(table.c.bucket_start).label("available_from"),
        func.max(table.c.bucket_start).label("available_to"),
    ).where(identifier_column == identifier_value)
    with energysaving_session() as session:
        row = session.execute(stmt).mappings().first()
    if not row:
        return None, None
    return row.get("available_from"), row.get("available_to")


def _query_sale_rows_for_table(
    table: Any,
    sale_id: int,
    range_start: datetime,
    range_end: datetime,
) -> list[dict[str, Any]]:
    columns = [table.c.bucket_start.label("bucket_start")]
    columns.extend(table.c[field].label(field) for field in _available_sale_fields(table))
    stmt = (
        select(*columns)
        .where(
            and_(
                table.c.idSala == sale_id,
                table.c.bucket_start >= range_start,
                table.c.bucket_start < range_end,
            )
        )
        .order_by(table.c.bucket_start.asc())
    )
    with energysaving_session() as session:
        return [dict(row._mapping) for row in session.execute(stmt).all()]


def _resolve_sale_table_for_selection(
    context: SaleContext,
    selection: RangeSelection,
) -> tuple[str, Any, tuple[datetime | None, datetime | None], list[dict[str, Any]]]:
    tables = get_sale_timeseries_tables()
    preferred_granularity = selection.granularity
    fallback_availability = (None, None)

    for granularity in iter_sale_granularity_candidates(preferred_granularity):
        table = tables.get(granularity)
        if table is None:
            continue

        availability = _query_bucket_availability(table, table.c.idSala, context.sale_id)
        available_from, available_to = availability
        if available_from is None or available_to is None:
            if granularity == preferred_granularity:
                fallback_availability = availability
            continue

        span = granularity_span(granularity)
        if selection.range_start < available_from:
            continue
        if available_to + span < selection.range_end:
            continue

        rows = _query_sale_rows_for_table(table, context.sale_id, selection.range_start, selection.range_end)
        if rows:
            return granularity, table, availability, rows
        if granularity == preferred_granularity:
            fallback_availability = availability

    preferred_table = tables.get(preferred_granularity)
    if preferred_table is None:
        raise RuntimeError(f"Aggregate table not available for granularity {preferred_granularity}.")

    rows = _query_sale_rows_for_table(preferred_table, context.sale_id, selection.range_start, selection.range_end)
    return preferred_granularity, preferred_table, fallback_availability, rows


def _weighted_average(rows: list[dict[str, Any]], field: str) -> float | None:
    valid = [row for row in rows if row.get(field) is not None]
    if not valid:
        return None

    weights = [float(row.get("samples_count") or 0) for row in valid]
    weighted_rows = [item for item in zip(valid, weights) if item[1] > 0]
    if weighted_rows:
        numerator = sum(float(row[field]) * weight for row, weight in weighted_rows)
        denominator = sum(weight for _, weight in weighted_rows)
        if denominator > 0:
            return numerator / denominator

    values = [float(row[field]) for row in valid]
    return sum(values) / len(values)


def _merge_sale_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "bucket_start": rows[0]["bucket_start"],
    }

    for field in ("samples_count", "energia_kwh_sum", "volume_nm3_sum"):
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        merged[field] = sum(values) if values else None

    for field in ("pressione_avg", "potenza_kw_avg", "flusso_nm3h_avg", "dewpoint_avg", "temperatura_avg", "cons_specifico_avg"):
        merged[field] = _weighted_average(rows, field)

    return merged


def _compress_sale_rows(rows: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    if max_points < 1:
        raise ValueError("max_points must be >= 1")
    if len(rows) <= max_points:
        return rows

    chunk_size = max(1, math.ceil(len(rows) / max_points))
    return [
        _merge_sale_rows(rows[index:index + chunk_size])
        for index in range(0, len(rows), chunk_size)
    ]


def _serialize_sale_point(row: dict[str, Any]) -> dict[str, Any]:
    power = row.get("potenza_kw_avg")
    flow = row.get("flusso_nm3h_avg")
    if power is not None and flow not in (None, 0):
        cons_specifico = float(power) / float(flow)
    else:
        cons_specifico = None

    return {
        "timestamp": _to_utc_iso(row.get("bucket_start")),
        "pressione": float(row["pressione_avg"]) if row.get("pressione_avg") is not None else None,
        "potenza_kw": float(power) if power is not None else None,
        "cons_specifico": cons_specifico,
        "flusso_nm3h": float(flow) if flow is not None else None,
        "dewpoint": float(row["dewpoint_avg"]) if row.get("dewpoint_avg") is not None else None,
        "temperatura": float(row["temperatura_avg"]) if row.get("temperatura_avg") is not None else None,
    }


def fetch_sale_chart_timeseries(
    identifier: str,
    range_key: str | None,
    from_ts: str | None,
    to_ts: str | None,
    max_points: int,
) -> dict[str, Any]:
    context = _resolve_sale_context(identifier)
    selection = resolve_range_selection(range_key, from_ts, to_ts)
    resolved_granularity, table, availability, rows = _resolve_sale_table_for_selection(context, selection)
    available_from, available_to = availability
    compressed_rows = _compress_sale_rows(rows, max_points=max_points)
    points = [_serialize_sale_point(row) for row in compressed_rows]

    return {
        "sale": context.sale_code,
        "sale_name": context.sale_name,
        "plant": context.plant_name,
        "last_update": context.last_update,
        "from_ts": _to_utc_iso(selection.range_start),
        "to_ts": _to_utc_iso(selection.range_end),
        "available_from_ts": _to_utc_iso(available_from),
        "available_to_ts": _to_utc_iso(available_to),
        "requested_range": selection.range_key,
        "granularity": resolved_granularity,
        "source_table": SALE_TIMESERIES_TABLES[resolved_granularity],
        "range_has_data": len(points) > 0,
        "points": points,
    }


def _compressor_current_state(row: dict[str, Any] | None) -> str:
    if not row:
        return "OFF"
    power = float(row.get("potAttiva") or 0)
    currents = [float(row.get(name) or 0) for name in ("l1", "l2", "l3")]
    voltages = [float(row.get(name) or 0) for name in ("u1", "u2", "u3")]
    if power > 0.5 or any(current > 1 for current in currents):
        return "ON"
    if any(voltage > 10 for voltage in voltages):
        return "STANDBY"
    return "OFF"


def _compressor_dominant_state(minutes_on: float, minutes_standby: float, minutes_off: float) -> str:
    state, _ = max(
        (("ON", minutes_on), ("STANDBY", minutes_standby), ("OFF", minutes_off)),
        key=lambda item: item[1],
    )
    return state


def _round_activity_minutes(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(round(value))


def fetch_sale_compressor_activity(
    identifier: str,
    range_key: str | None,
    from_ts: str | None,
    to_ts: str | None,
) -> dict[str, Any]:
    context = _resolve_sale_context(identifier)
    selection = resolve_range_selection(range_key, from_ts, to_ts)
    compressor_granularity = choose_compressor_activity_granularity(selection.granularity)
    table = get_compressor_activity_tables().get(compressor_granularity)
    if table is None:
        raise RuntimeError(f"Compressor aggregate table not available for granularity {compressor_granularity}.")
    available_from, available_to = _query_bucket_availability(table, table.c.idSala, context.sale_id)

    schema = get_energysaving_schema()
    power_weights = table.c.samples_count if "samples_count" in table.c else None
    avg_power_expr = (
        (func.sum(table.c.potenza_kw_avg * power_weights) / func.nullif(func.sum(power_weights), 0))
        if power_weights is not None and "potenza_kw_avg" in table.c
        else func.avg(table.c.potenza_kw_avg) if "potenza_kw_avg" in table.c else None
    )

    selected_columns = [
        schema.compressori.c.idCompressore.label("id_compressore"),
        func.coalesce(schema.compressori.c.codice, schema.compressori.c.codifica, func.concat("C", schema.compressori.c.idCompressore)).label("code"),
        func.coalesce(schema.compressori.c.nome, schema.compressori.c.codice, schema.compressori.c.codifica).label("name"),
        func.coalesce(func.sum(table.c.minuti_on), 0).label("minutes_on"),
        func.coalesce(func.sum(table.c.minuti_standby), 0).label("minutes_standby"),
        func.coalesce(func.sum(table.c.minuti_off), 0).label("minutes_off"),
    ]
    if "energia_kwh_sum" in table.c:
        selected_columns.append(func.coalesce(func.sum(table.c.energia_kwh_sum), 0).label("energy_kwh"))
    if avg_power_expr is not None:
        selected_columns.append(avg_power_expr.label("avg_power_kw"))

    stmt = (
        select(*selected_columns)
        .select_from(table.join(schema.compressori, schema.compressori.c.idCompressore == table.c.idCompressore))
        .where(
            and_(
                table.c.idSala == context.sale_id,
                table.c.bucket_start >= selection.range_start,
                table.c.bucket_start < selection.range_end,
            )
        )
        .group_by(schema.compressori.c.idCompressore, schema.compressori.c.codice, schema.compressori.c.codifica, schema.compressori.c.nome)
        .order_by(schema.compressori.c.idCompressore.asc())
    )

    current_stmt = (
        select(
            schema.stato_compressori_corrente.c.idCompressore,
            schema.stato_compressori_corrente.c.potAttiva,
            schema.stato_compressori_corrente.c.u1,
            schema.stato_compressori_corrente.c.u2,
            schema.stato_compressori_corrente.c.u3,
            schema.stato_compressori_corrente.c.l1,
            schema.stato_compressori_corrente.c.l2,
            schema.stato_compressori_corrente.c.l3,
        )
        .where(schema.stato_compressori_corrente.c.idSala == context.sale_id)
    )

    with energysaving_session() as session:
        rows = [dict(row._mapping) for row in session.execute(stmt).all()]
        current_rows = {
            int(row._mapping["idCompressore"]): dict(row._mapping)
            for row in session.execute(current_stmt).all()
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        minutes_on = float(row.get("minutes_on") or 0)
        minutes_standby = float(row.get("minutes_standby") or 0)
        minutes_off = float(row.get("minutes_off") or 0)
        display_minutes_on = _round_activity_minutes(minutes_on)
        display_minutes_standby = _round_activity_minutes(minutes_standby)
        display_minutes_off = _round_activity_minutes(minutes_off)
        total_minutes = max(minutes_on + minutes_standby + minutes_off, 0.0)
        utilization_pct = (minutes_on / total_minutes * 100.0) if total_minutes > 0 else 0.0
        standby_pct = (minutes_standby / total_minutes * 100.0) if total_minutes > 0 else 0.0
        off_pct = (minutes_off / total_minutes * 100.0) if total_minutes > 0 else 0.0
        compressor_id = int(row["id_compressore"])
        items.append(
            {
                "id_compressore": compressor_id,
                "code": str(row.get("code") or compressor_id),
                "name": str(row.get("name") or row.get("code") or compressor_id),
                "current_state": _compressor_current_state(current_rows.get(compressor_id)),
                "dominant_state": _compressor_dominant_state(minutes_on, minutes_standby, minutes_off),
                "minutes_on": display_minutes_on,
                "minutes_standby": display_minutes_standby,
                "minutes_off": display_minutes_off,
                "utilization_pct": utilization_pct,
                "standby_pct": standby_pct,
                "off_pct": off_pct,
                "energy_kwh": float(row["energy_kwh"]) if row.get("energy_kwh") is not None else None,
                "avg_power_kw": float(row["avg_power_kw"]) if row.get("avg_power_kw") is not None else None,
            }
        )

    return {
        "sale": context.sale_code,
        "sale_name": context.sale_name,
        "plant": context.plant_name,
        "from_ts": _to_utc_iso(selection.range_start),
        "to_ts": _to_utc_iso(selection.range_end),
        "available_from_ts": _to_utc_iso(available_from),
        "available_to_ts": _to_utc_iso(available_to),
        "requested_range": selection.range_key,
        "granularity": compressor_granularity,
        "source_table": COMPRESSOR_ACTIVITY_TABLES[compressor_granularity],
        "range_has_data": len(items) > 0,
        "items": items,
    }
