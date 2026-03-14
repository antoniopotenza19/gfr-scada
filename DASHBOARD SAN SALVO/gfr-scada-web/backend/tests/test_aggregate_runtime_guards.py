from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import os
import tempfile
from types import SimpleNamespace

from sqlalchemy import Column, DateTime, Integer, MetaData, Numeric, Table, create_engine
from sqlalchemy.orm import sessionmaker

from app.services import aggregate_rollups, energysaving_runtime
from app.services.sale_charts import SaleContext, fetch_sale_chart_timeseries


def _build_runtime_test_db():
    db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_file.close()

    engine = create_engine(
        f"sqlite:///{db_file.name}",
        connect_args={"check_same_thread": False},
        future=True,
    )
    metadata = MetaData()
    registrazioni_sale = Table(
        "registrazioni_sale",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("idSala", Integer, nullable=False),
        Column("timestamp", DateTime, nullable=False),
        Column("pressione", Numeric(18, 4), nullable=True),
    )
    sale_agg_1h = Table(
        "sale_agg_1h",
        metadata,
        Column("idSala", Integer, primary_key=True, nullable=False),
        Column("bucket_start", DateTime, primary_key=True, nullable=False),
        Column("samples_count", Integer, nullable=True),
        Column("pressione_avg", Numeric(18, 4), nullable=True),
        Column("cons_specifico_avg", Numeric(18, 4), nullable=True),
        Column("energia_kwh_sum", Numeric(18, 4), nullable=True),
        Column("volume_nm3_sum", Numeric(18, 4), nullable=True),
    )
    metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    schema = SimpleNamespace(registrazioni_sale=registrazioni_sale)
    return db_file.name, engine, SessionLocal, schema, sale_agg_1h


def test_sale_raw_specific_consumption_expression_has_no_raw_average_fallback():
    expressions = aggregate_rollups.build_sale_raw_select_expressions(
        {"cons_specifico_avg", "volume_nm3_sum", "energia_kwh_sum"},
        "1min",
    )

    assert "cons_specifico_avg" in expressions
    assert "AVG(src.`consSpecifico`)" not in expressions["cons_specifico_avg"]
    assert "ELSE NULL END" in expressions["cons_specifico_avg"]


def test_sale_pressure2_expression_is_available_in_raw_and_rollup_layers():
    raw_expressions = aggregate_rollups.build_sale_raw_select_expressions(
        {"pressione_avg", "pressione2_avg", "temperatura_avg", "temperatura2_avg"},
        "1min",
    )
    rollup_expressions = aggregate_rollups.build_sale_rollup_select_expressions(
        {"pressione_avg", "pressione2_avg", "temperatura_avg", "temperatura2_avg"},
    )

    assert raw_expressions["pressione2_avg"] == "AVG(src.`pressione2`)"
    assert raw_expressions["temperatura2_avg"] == "AVG(src.`temperatura2`)"
    assert "pressione2_avg" in rollup_expressions
    assert "src.`pressione2_avg`" in rollup_expressions["pressione2_avg"]
    assert "temperatura2_avg" in rollup_expressions
    assert "src.`temperatura2_avg`" in rollup_expressions["temperatura2_avg"]


def test_compressori_raw_minutes_are_duration_based_and_not_sample_ratio():
    expressions = aggregate_rollups.build_compressori_raw_select_expressions(
        {"minuti_on", "minuti_standby", "minuti_off"},
        "rollup_bucket_start",
    )

    assert "COUNT(*)" not in expressions["minuti_on"]
    assert "COUNT(*)" not in expressions["minuti_standby"]
    assert "COUNT(*)" not in expressions["minuti_off"]
    assert "TIMESTAMPDIFF(MICROSECOND" in expressions["minuti_on"]
    assert "rollup_next_timestamp" in expressions["minuti_on"]
    assert "rollup_bucket_rownum" in expressions["minuti_on"]


def test_dashboard_aggregate_timeseries_uses_weighted_average_for_multi_sala(monkeypatch):
    db_path, engine, SessionLocal, schema, sale_agg_1h = _build_runtime_test_db()

    with SessionLocal.begin() as session:
        session.execute(
            sale_agg_1h.insert(),
            [
                {
                    "idSala": 1,
                    "bucket_start": datetime(2026, 1, 10, 0, 0, 0),
                    "samples_count": 1,
                    "pressione_avg": 1.0,
                    "cons_specifico_avg": 1.0,
                    "energia_kwh_sum": 10.0,
                    "volume_nm3_sum": 10.0,
                },
                {
                    "idSala": 2,
                    "bucket_start": datetime(2026, 1, 10, 0, 0, 0),
                    "samples_count": 9,
                    "pressione_avg": 9.0,
                    "cons_specifico_avg": 0.2,
                    "energia_kwh_sum": 18.0,
                    "volume_nm3_sum": 90.0,
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
    monkeypatch.setattr(energysaving_runtime, "get_sale_aggregate_tables", lambda: {"1h": sale_agg_1h})
    monkeypatch.setattr(energysaving_runtime, "aggregate_granularity_span", lambda granularity: timedelta(days=365))
    monkeypatch.setattr(energysaving_runtime, "choose_aggregate_granularity", lambda *args, **kwargs: "1h")
    monkeypatch.setattr(
        energysaving_runtime,
        "resolve_target",
        lambda session, identifier: energysaving_runtime.ResolvedTarget(
            scope="impianto",
            label="San Salvo",
            sala_ids=[1, 2],
            sala_codes=["S1", "S2"],
        ),
    )

    pressure_points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="SAN SALVO",
        signal="Pressione",
        from_ts="2026-01-10T00:00:00Z",
        to_ts="2026-01-11T00:00:00Z",
        minutes=60,
        max_points=100,
        bucket=None,
        agg="avg",
    )
    cs_points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="SAN SALVO",
        signal="Consumo Specifico",
        from_ts="2026-01-10T00:00:00Z",
        to_ts="2026-01-11T00:00:00Z",
        minutes=60,
        max_points=100,
        bucket=None,
        agg="avg",
    )

    assert len(pressure_points) == 1
    assert abs(pressure_points[0]["value"] - 8.2) < 1e-9
    assert len(cs_points) == 1
    assert abs(cs_points[0]["value"] - 0.28) < 1e-9

    engine.dispose()
    os.unlink(db_path)


def test_historical_timeseries_request_does_not_fallback_to_raw(monkeypatch):
    called = {"resolve_room_signal_column": False}

    monkeypatch.setattr(energysaving_runtime, "query_aggregate_timeseries", lambda *args, **kwargs: None)

    def _unexpected_column_lookup(signal: str):
        called["resolve_room_signal_column"] = True
        raise AssertionError("raw fallback should stay disabled for historical standard requests")

    monkeypatch.setattr(energysaving_runtime, "resolve_room_signal_column", _unexpected_column_lookup)

    points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="SAN SALVO",
        signal="Flusso TOT",
        from_ts="2025-11-01T00:00:00Z",
        to_ts="2026-03-13T00:00:00Z",
        minutes=60,
        max_points=240,
        bucket="1 month",
        agg="sum",
    )

    assert points == []
    assert called["resolve_room_signal_column"] is False


def test_short_live_timeseries_request_can_still_fallback_to_raw(monkeypatch):
    db_path, engine, SessionLocal, schema, sale_agg_1h = _build_runtime_test_db()
    del sale_agg_1h
    sample_ts = datetime.utcnow() - timedelta(minutes=1)

    with SessionLocal.begin() as session:
        session.execute(
            schema.registrazioni_sale.insert().values(
                idSala=1,
                timestamp=sample_ts,
                pressione=7.7,
            )
        )

    @contextmanager
    def test_session():
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    called = {"resolve_room_signal_column": False}

    monkeypatch.setattr(energysaving_runtime, "query_aggregate_timeseries", lambda *args, **kwargs: None)
    monkeypatch.setattr(energysaving_runtime, "energysaving_session", test_session)
    monkeypatch.setattr(energysaving_runtime, "get_energysaving_schema", lambda: schema)
    monkeypatch.setattr(
        energysaving_runtime,
        "resolve_target",
        lambda session, identifier: energysaving_runtime.ResolvedTarget(
            scope="sala",
            label="LAMINATI",
            sala_ids=[1],
            sala_codes=["LAMINATI"],
        ),
    )

    def _column_lookup(signal: str):
        called["resolve_room_signal_column"] = True
        return "pressione"

    monkeypatch.setattr(energysaving_runtime, "resolve_room_signal_column", _column_lookup)

    points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="LAMINATI",
        signal="Pressione",
        from_ts=None,
        to_ts=None,
        minutes=5,
        max_points=10,
        bucket=None,
        agg="avg",
    )

    assert called["resolve_room_signal_column"] is True
    assert len(points) == 1
    assert abs(points[0]["value"] - 7.7) < 1e-9

    engine.dispose()
    os.unlink(db_path)


def test_aggregate_timeseries_uses_end_exclusive_window(monkeypatch):
    db_path, engine, SessionLocal, schema, sale_agg_1h = _build_runtime_test_db()
    del schema

    with SessionLocal.begin() as session:
        session.execute(
            sale_agg_1h.insert(),
            [
                {
                    "idSala": 1,
                    "bucket_start": datetime(2026, 1, 1, 0, 0, 0),
                    "samples_count": 1,
                    "pressione_avg": 6.0,
                    "cons_specifico_avg": None,
                    "energia_kwh_sum": 1.0,
                    "volume_nm3_sum": 1.0,
                },
                {
                    "idSala": 1,
                    "bucket_start": datetime(2026, 1, 1, 2, 0, 0),
                    "samples_count": 1,
                    "pressione_avg": 9.0,
                    "cons_specifico_avg": None,
                    "energia_kwh_sum": 1.0,
                    "volume_nm3_sum": 1.0,
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
    monkeypatch.setattr(energysaving_runtime, "get_sale_aggregate_tables", lambda: {"1h": sale_agg_1h})
    monkeypatch.setattr(energysaving_runtime, "aggregate_granularity_span", lambda granularity: timedelta(days=365))
    monkeypatch.setattr(energysaving_runtime, "choose_aggregate_granularity", lambda *args, **kwargs: "1h")
    monkeypatch.setattr(
        energysaving_runtime,
        "resolve_target",
        lambda session, identifier: energysaving_runtime.ResolvedTarget(
            scope="sala",
            label="SS1",
            sala_ids=[1],
            sala_codes=["SS1"],
        ),
    )

    points = energysaving_runtime.fetch_dashboard_timeseries(
        identifier="SS1",
        signal="Pressione",
        from_ts="2026-01-01T00:00:00Z",
        to_ts="2026-01-01T02:00:00Z",
        minutes=120,
        max_points=10,
        bucket=None,
        agg="avg",
    )

    assert len(points) == 1
    assert points[0]["ts"] == "2026-01-01T00:00:00Z"

    engine.dispose()
    os.unlink(db_path)


def test_sale_chart_timeseries_serializes_secondary_pressure(monkeypatch):
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
        Column("pressione2_avg", Numeric(18, 4), nullable=True),
        Column("temperatura_avg", Numeric(18, 4), nullable=True),
        Column("temperatura2_avg", Numeric(18, 4), nullable=True),
    )
    metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    with SessionLocal.begin() as session:
        session.execute(
            sale_agg_1h.insert(),
            [
                {
                    "idSala": 2,
                    "bucket_start": datetime(2026, 2, 14, 16, 0, 0),
                    "samples_count": 60,
                    "pressione_avg": 3.5,
                    "pressione2_avg": 5.2,
                    "temperatura_avg": 28.1,
                    "temperatura2_avg": 31.4,
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

    monkeypatch.setattr("app.services.sale_charts.energysaving_session", test_session)
    monkeypatch.setattr("app.services.sale_charts.get_sale_timeseries_tables", lambda: {"1h": sale_agg_1h})
    monkeypatch.setattr("app.services.sale_charts.choose_sale_granularity_for_window", lambda *_args, **_kwargs: "1h")
    monkeypatch.setattr("app.services.sale_charts.iter_sale_granularity_candidates", lambda _granularity: ("1h",))
    monkeypatch.setattr("app.services.sale_charts.granularity_span", lambda _granularity: timedelta(days=365))
    monkeypatch.setattr(
        "app.services.sale_charts._resolve_sale_context",
        lambda _identifier: SaleContext(
            sale_id=2,
            sale_code="SS2",
            sale_name="SS2",
            plant_name="SAN SALVO",
            last_update=None,
        ),
    )

    payload = fetch_sale_chart_timeseries(
        identifier="SS2",
        range_key=None,
        from_ts="2026-02-14T00:00:00Z",
        to_ts="2026-02-15T00:00:00Z",
        max_points=120,
    )

    assert len(payload["points"]) == 1
    assert payload["points"][0]["pressione"] == 3.5
    assert payload["points"][0]["pressione2"] == 5.2
    assert payload["points"][0]["temperatura"] == 28.1
    assert payload["points"][0]["temperatura2"] == 31.4

    engine.dispose()
    os.unlink(db_file.name)
