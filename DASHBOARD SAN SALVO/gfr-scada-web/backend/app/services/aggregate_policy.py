from __future__ import annotations

from datetime import UTC, datetime, timedelta

SALE_GRANULARITY_ORDER: tuple[str, ...] = ("1min", "15min", "1h", "1d", "1month")

SALE_PRESET_TO_GRANULARITY: dict[str, str] = {
    "5m": "1min",
    "15m": "1min",
    "30m": "1min",
    "1h": "1min",
    "1d": "15min",
    "1w": "1h",
    "1mo": "1h",
    "3mo": "1h",
    "6mo": "1d",
    "1y": "1d",
    "3y": "1d",
}

SALE_REALTIME_PRESETS = frozenset({"5m", "15m", "30m", "1h"})


def granularity_span(granularity: str) -> timedelta:
    if granularity == "1month":
        return timedelta(days=31)
    if granularity == "1d":
        return timedelta(days=1)
    if granularity == "1h":
        return timedelta(hours=1)
    if granularity == "15min":
        return timedelta(minutes=15)
    if granularity == "1min":
        return timedelta(minutes=1)
    raise ValueError(f"Unsupported granularity: {granularity}")


def choose_sale_granularity_for_window(range_start: datetime, range_end: datetime) -> str:
    span = range_end - range_start
    if span <= timedelta(hours=1):
        return "1min"
    if span <= timedelta(days=1):
        return "15min"
    if span <= timedelta(days=90):
        return "1h"
    if span <= timedelta(days=365 * 3):
        return "1d"
    return "1month"


def choose_sale_granularity_for_request(
    explicit_granularity: str | None,
    from_value: datetime | None,
    to_value: datetime | None,
    minutes: int,
) -> str:
    if explicit_granularity is not None:
        return explicit_granularity

    end_value = to_value or datetime.now(UTC).replace(tzinfo=None)
    start_value = from_value or (end_value - timedelta(minutes=minutes))
    return choose_sale_granularity_for_window(start_value, end_value)


def iter_sale_granularity_candidates(preferred_granularity: str) -> tuple[str, ...]:
    try:
        start_index = SALE_GRANULARITY_ORDER.index(preferred_granularity)
    except ValueError as exc:
        raise ValueError(f"Unsupported sale granularity: {preferred_granularity}") from exc
    return SALE_GRANULARITY_ORDER[start_index:]


def choose_compressor_activity_granularity(sale_granularity: str) -> str:
    return "1min" if sale_granularity in {"1min", "15min"} else "1h"
