from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import os
import tempfile

from sqlalchemy import Column, DateTime, Integer, MetaData, Numeric, String, Table, create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.scripts.csv_to_db_ingestor import (
    CompressorIdentity,
    DatabaseAdapter,
    ParseResult,
    ParsedCompressoreRecord,
    ParsedSalaRecord,
    RemoteCsvFile,
    SchemaBundle,
)
from app.services import energysaving_runtime


def _build_energysaving_test_db():
    db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_file.close()

    engine = create_engine(
        f"sqlite:///{db_file.name}",
        connect_args={"check_same_thread": False},
        future=True,
    )
    metadata = MetaData()
    Table(
        "impianti",
        metadata,
        Column("idImpianto", Integer, primary_key=True),
        Column("nome", String(100), nullable=False),
        Column("descrizione", String(255), nullable=True),
    )
    Table(
        "sale",
        metadata,
        Column("idSala", Integer, primary_key=True),
        Column("idImpianto", Integer, nullable=False),
        Column("codice", String(50), nullable=False),
        Column("nome", String(100), nullable=False),
        Column("abilitato", Integer, nullable=False, server_default="1"),
    )
    Table(
        "compressori",
        metadata,
        Column("idCompressore", Integer, primary_key=True),
        Column("idSala", Integer, nullable=False),
        Column("codice", String(50), nullable=True),
        Column("codifica", String(50), nullable=True),
        Column("nome", String(100), nullable=True),
        Column("abilitato", Integer, nullable=False, server_default="1"),
    )
    Table(
        "registrazioni_sale",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("idSala", Integer, nullable=False),
        Column("timestamp", DateTime, nullable=False),
        Column("pressione", Numeric(18, 4), nullable=True),
        Column("potAttTotale", Numeric(18, 4), nullable=True),
        Column("consSpecifico", Numeric(18, 4), nullable=True),
        Column("statoDati", Integer, nullable=True),
        Column("created_at", DateTime, nullable=True, server_default=func.now()),
    )
    Table(
        "registrazioni_compressori",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("idCompressore", Integer, nullable=False),
        Column("idSala", Integer, nullable=False),
        Column("timestamp", DateTime, nullable=False),
        Column("potAttiva", Numeric(18, 4), nullable=True),
        Column("l1", Numeric(18, 4), nullable=True),
        Column("l2", Numeric(18, 4), nullable=True),
        Column("l3", Numeric(18, 4), nullable=True),
        Column("cosphi", Numeric(18, 4), nullable=True),
        Column("energiaAttivaTotale", Numeric(18, 4), nullable=True),
        Column("statoCompressore", Integer, nullable=True),
        Column("statoDati", Integer, nullable=True),
        Column("created_at", DateTime, nullable=True, server_default=func.now()),
    )
    metadata.create_all(bind=engine)

    schema = SchemaBundle(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return db_file.name, engine, SessionLocal, schema


def test_insert_parse_result_updates_current_state_without_touching_history():
    db_path, engine, SessionLocal, schema = _build_energysaving_test_db()
    adapter = DatabaseAdapter(SessionLocal, schema)

    with SessionLocal.begin() as session:
        session.execute(schema.impianti.insert().values(idImpianto=1, nome="San Salvo", descrizione=""))
        session.execute(schema.sale.insert().values(idSala=1, idImpianto=1, codice="SS1", nome="SS1", abilitato=1))
        session.execute(
            schema.compressori.insert().values(
                idCompressore=10,
                idSala=1,
                codice="COMP_1",
                codifica="COMP_1",
                nome="COMP 1",
                abilitato=1,
            )
        )

    identity = CompressorIdentity(
        raw_alias="COMP 1",
        normalized_name="COMP 1",
        codice="COMP_1",
        tipologia="compressore",
    )
    ts_old = datetime(2026, 3, 10, 10, 0, 0)
    ts_new = datetime(2026, 3, 10, 10, 15, 0)

    first_result = ParseResult(
        rows_seen=2,
        rows_valid_timestamp=2,
        sala_records=[
            ParsedSalaRecord(timestamp=ts_old, metrics={"pressione": 6.1, "potAttTotale": 100.0, "consSpecifico": 0.9}),
            ParsedSalaRecord(timestamp=ts_new, metrics={"pressione": 7.2, "potAttTotale": 110.0, "consSpecifico": 1.1}),
        ],
        compressore_records=[
            ParsedCompressoreRecord(timestamp=ts_old, identity=identity, metrics={"potAttiva": 50.0, "statoCompressore": 1}),
            ParsedCompressoreRecord(timestamp=ts_new, identity=identity, metrics={"potAttiva": 55.0, "statoCompressore": 1}),
        ],
    )

    adapter.insert_parse_result(
        source=RemoteCsvFile("SS1.csv", "http://dummy/SS1.csv", "http://dummy"),
        mode="poll",
        sala_id=1,
        sala_code="SS1",
        parse_result=first_result,
    )

    with SessionLocal() as session:
        live_room = session.execute(select(schema.stato_sale_corrente)).mappings().one()
        live_compressor = session.execute(select(schema.stato_compressori_corrente)).mappings().one()
        assert live_room["timestamp"] == ts_new
        assert float(live_room["pressione"]) == 7.2
        assert live_compressor["timestamp"] == ts_new
        assert float(live_compressor["potAttiva"]) == 55.0
        assert session.execute(select(func.count()).select_from(schema.registrazioni_sale)).scalar_one() == 2
        assert session.execute(select(func.count()).select_from(schema.registrazioni_compressori)).scalar_one() == 2

    older_result = ParseResult(
        rows_seen=1,
        rows_valid_timestamp=1,
        sala_records=[
            ParsedSalaRecord(
                timestamp=datetime(2026, 3, 9, 23, 59, 45),
                metrics={"pressione": 4.4, "potAttTotale": 80.0, "consSpecifico": 0.8},
            )
        ],
        compressore_records=[
            ParsedCompressoreRecord(
                timestamp=datetime(2026, 3, 9, 23, 59, 45),
                identity=identity,
                metrics={"potAttiva": 35.0, "statoCompressore": 0},
            )
        ],
    )

    adapter.insert_parse_result(
        source=RemoteCsvFile("SS1-old.csv", "http://dummy/SS1-old.csv", "http://dummy"),
        mode="backfill",
        sala_id=1,
        sala_code="SS1",
        parse_result=older_result,
    )

    with SessionLocal() as session:
        live_room = session.execute(select(schema.stato_sale_corrente)).mappings().one()
        live_compressor = session.execute(select(schema.stato_compressori_corrente)).mappings().one()
        assert live_room["timestamp"] == ts_new
        assert float(live_room["pressione"]) == 7.2
        assert live_compressor["timestamp"] == ts_new
        assert float(live_compressor["potAttiva"]) == 55.0
        assert session.execute(select(func.count()).select_from(schema.registrazioni_sale)).scalar_one() == 3
        assert session.execute(select(func.count()).select_from(schema.registrazioni_compressori)).scalar_one() == 3

    engine.dispose()
    os.unlink(db_path)


def test_fetch_dashboard_summary_reads_current_state_tables(monkeypatch):
    db_path, engine, SessionLocal, schema = _build_energysaving_test_db()

    room_ts = datetime(2026, 3, 10, 12, 0, 0)
    compressor_ts = datetime(2026, 3, 10, 12, 0, 5)

    with SessionLocal.begin() as session:
        session.execute(schema.impianti.insert().values(idImpianto=1, nome="San Salvo", descrizione=""))
        session.execute(schema.sale.insert().values(idSala=1, idImpianto=1, codice="SS1", nome="SS1", abilitato=1))
        session.execute(
            schema.compressori.insert().values(
                idCompressore=10,
                idSala=1,
                codice="COMP_1",
                codifica="COMP_1",
                nome="COMP 1",
                abilitato=1,
            )
        )
        session.execute(
            schema.registrazioni_sale.insert().values(
                idSala=1,
                timestamp=datetime(2026, 3, 10, 12, 30, 0),
                pressione=1.1,
                potAttTotale=20.0,
                consSpecifico=0.3,
                statoDati=1,
                created_at=datetime(2026, 3, 10, 12, 30, 0),
            )
        )
        session.execute(
            schema.registrazioni_compressori.insert().values(
                idCompressore=10,
                idSala=1,
                timestamp=datetime(2026, 3, 10, 12, 30, 0),
                potAttiva=10.0,
                statoCompressore=0,
                statoDati=1,
                created_at=datetime(2026, 3, 10, 12, 30, 0),
            )
        )
        session.execute(
            schema.stato_sale_corrente.insert().values(
                idSala=1,
                timestamp=room_ts,
                pressione=8.8,
                potAttTotale=120.0,
                consSpecifico=1.4,
                statoDati=1,
                updated_at=room_ts,
            )
        )
        session.execute(
            schema.stato_compressori_corrente.insert().values(
                idCompressore=10,
                idSala=1,
                timestamp=compressor_ts,
                potAttiva=65.0,
                statoCompressore=1,
                statoDati=1,
                updated_at=compressor_ts,
            )
        )

    @contextmanager
    def test_session():
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(energysaving_runtime, "get_energysaving_schema", lambda: schema)
    monkeypatch.setattr(energysaving_runtime, "energysaving_session", test_session)
    monkeypatch.setattr(
        energysaving_runtime,
        "get_sale_catalog",
        lambda: (
            {"idSala": 1, "idImpianto": 1, "codice": "SS1", "nome": "SS1", "abilitato": 1},
        ),
    )
    monkeypatch.setattr(energysaving_runtime, "get_impianto_catalog", lambda: {1: "San Salvo"})

    payload = energysaving_runtime.fetch_dashboard_summary("SS1")

    assert payload is not None
    assert payload["plant"] == "SS1"
    assert payload["signals"]["Pressione"]["value"] == 8.8
    assert payload["signals"]["Potenza Attiva COMP 1"]["value"] == 65.0
    assert payload["last_update"] == "2026-03-10T12:00:05Z"

    engine.dispose()
    os.unlink(db_path)
