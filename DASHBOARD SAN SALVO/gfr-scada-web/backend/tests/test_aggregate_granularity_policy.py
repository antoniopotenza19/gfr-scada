from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import os
import tempfile

from sqlalchemy import Column, DateTime, Integer, MetaData, Numeric, Table, create_engine
from sqlalchemy.orm import sessionmaker

from app.services import energysaving_runtime, sale_charts
from app.services.aggregate_policy import (
    choose_compressor_activity_granularity,
    choose_sale_granularity_for_window,
    iter_sale_granularity_candidates,
)


def test_choose_sale_granularity_for_window_short_medium_and_long_ranges():
    assert choose_sale_granularity_for_window(datetime(2026, 3, 12, 10, 0), datetime(2026, 3, 12, 10, 30)) == "1min"
    assert choose_sale_granularity_for_window(datetime(2026, 3, 11, 10, 0), datetime(2026, 3, 12, 10, 0)) == "15min"
    assert choose_sale_granularity_for_window(datetime(2026, 2, 1, 0, 0), datetime(2026, 3, 12, 0, 0)) == "1h"
    assert choose_sale_granularity_for_window(datetime(2024, 3, 12, 0, 0), datetime(2026, 3, 12, 0, 0)) == "1d"
    assert choose_sale_granularity_for_window(datetime(2018, 3, 12, 0, 0), datetime(2026, 3, 12, 0, 0)) == "1month"


def test_iter_sale_granularity_candidates_moves_only_to_coarser_layers():
    assert iter_sale_granularity_candidates("1h") == ("1h", "1d", "1month")
    assert iter_sale_granularity_candidates("1d") == ("1d", "1month")
    assert choose_compressor_activity_granularity("15min") == "1min"
    assert choose_compressor_activity_granularity("1month") == "1h"


def test_sale_chart_range_selection_supports_three_year_preset():
    selection = sale_charts.resolve_range_selection("3y", None, None)
    assert selection.granularity == "1d"
    assert selection.realtime is False


def test_dashboard_timeseries_falls_back_to_coarser_aggregate_when_fine_layer_is_not_covered(monkeypatch):
    db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_file.close()
    engine = create_engine(
        f"sqlite:///{db_file.name}",
        connect_args={"check_same_thread": False},
        future=True,
    )
    metadata = MetaData()
    sale_agg_1h = Table(
        "sale_agg_1h",
        metadata,
        Column("idSala", Integer, primary_key=True, nullable=False),
        Column("bucket_start", DateTime, primary_key=True, nullable=False),
        Column("samples_count", Integer, nullable=True),
        Column("pressione_avg", Numeric(18, 4), nullable=True),
    )
    sale_agg_1d = Table(
        "sale_agg_1d",
        metadata,
        Column("idSala", Integer, primary_key=True, nullable=False),
        Column("bucket_start", DateTime, primary_key=True, nullable=False),
        Column("samples_count", Integer, nullable=True),
        Column("pressione_avg", Numeric(18, 4), nullable=True),
    )
    metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    with SessionLocal.begin() as session:
        session.execute(
            sale_agg_1d.insert(),
            [
                {
                    "idSala": 1,
                    "bucket_start": datetime(2018, 1, 1, 0, 0, 0),
                    "samples_count": 24,
                    "pressione_avg": 7.5,
                },
                {
                    "idSala": 1,
                    "bucket_start": datetime(2018, 1, 2, 0, 0, 0),
                    "samples_count": 24,
                    "pressione_avg": 7.7,
                },
            ],
        )

    @contextmanager
    def test_session():
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(energysaving_runtime, "energysaving_session", test_session)
    monkeypatch.setattr(
        energysaving_runtime,
        "get_sale_aggregate_tables",
        lambda: {"1h": sale_agg_1h, "1d": sale_agg_1d},
    )
    monkeypatch.setattr(
        energysaving_runtime,
        "resolve_target",
        lambda session, identifier: energysaving_runtime.ResolvedTarget(
            scope="sala",
            label="LAMINATO",
            sala_ids=[1],
            sala_codes=["LAMINATO"],
        ),
    )

    points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="LAMINATO",
        signal="Pressione",
        from_ts="2018-01-01T00:00:00Z",
        to_ts="2018-01-03T00:00:00Z",
        minutes=60,
        max_points=100,
        bucket=None,
        agg="avg",
    )

    assert len(points) == 2
    assert points[0]["ts"] == "2018-01-01T00:00:00Z"

    engine.dispose()
    os.unlink(db_file.name)
