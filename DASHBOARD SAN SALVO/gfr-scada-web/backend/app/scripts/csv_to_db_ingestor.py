"""
CSV -> MySQL ingestor for the `gfr_energysaving` schema.

Usage examples:
  python backend/app/scripts/csv_to_db_ingestor.py --once
  python backend/app/scripts/csv_to_db_ingestor.py --once --target-date 2026-03-08
  python backend/app/scripts/csv_to_db_ingestor.py
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus, unquote, urljoin, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import (
    TIMESTAMP,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    and_,
    create_engine,
    delete,
    func,
    inspect,
    or_,
    select,
)
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.schema import Table
from zoneinfo import ZoneInfo

load_dotenv()

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.services.aggregate_rollups import (
    ensure_sale_aggregate_secondary_columns,
    level_spec_for_granularity,
    refresh_pyramid_range_safely,
    validate_rollup_schema,
)

LOGGER = logging.getLogger("csv_to_db_ingestor")

DEFAULT_POLL_SECONDS = 5
DEFAULT_LOG_LEVEL = "INFO"
TARGET_DATABASE_NAME = "gfr_energysaving"
DEFAULT_BASE_CSV_URL = os.getenv("BASE_CSV_URL", "http://94.138.172.234:46812/shared").rstrip("/")
SOURCE_TIMEZONE = os.getenv("INGEST_SOURCE_TIMEZONE", "Europe/Rome").strip() or "Europe/Rome"
REMOTE_TIMEOUT_SECONDS = 20
POLL_LOOKBACK_DAYS = max(1, int(os.getenv("SCADA_POLL_LOOKBACK_DAYS", "2")))
SOURCE_RECHECK_SECONDS = max(
    1,
    int(
        os.getenv(
            "SCADA_SOURCE_RECHECK_SECONDS",
            os.getenv("SCADA_POLL_SECONDS", str(DEFAULT_POLL_SECONDS)),
        )
    ),
)
VERBOSE_COLUMN_DEBUG = os.getenv("SCADA_VERBOSE_COLUMN_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
PIPELINE_DEBUG_ENABLED = os.getenv("SCADA_PIPELINE_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_STATO_DATI = int(os.getenv("SCADA_DEFAULT_STATO_DATI", "1"))
DEFAULT_STATUS_FILE = os.path.join(os.path.dirname(__file__), "csv_to_db_ingestor.status.json")

HARDCODED_BASE_CSV_URL = DEFAULT_BASE_CSV_URL

HREF_RE = re.compile(r'href=[\'"]([^\'"]+)[\'"]', re.IGNORECASE)


def clean_duplicate_suffix(value: str) -> str:
    text = value.replace("\ufeff", "").replace("Â", "").strip()
    match = re.search(r"\.(\d+)$", text)
    if match:
        suffix = int(match.group(1)) + 1
        text = f"{text[:match.start()].strip()} ({suffix})"
    return re.sub(r"\s+", " ", text)


def normalize_lookup_key(value: str) -> str:
    cleaned = clean_duplicate_suffix(value)
    normalized = unicodedata.normalize("NFKD", cleaned)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = normalized.replace("^", "")
    normalized = re.sub(r"m\s*3", "m3", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def normalize_source_filename(value: str) -> str:
    basename = os.path.basename(value).strip()
    stem = re.sub(r"\.[^.]+$", "", basename)
    stem = re.sub(r"^\d{2}-\d{2}-", "", stem)
    return normalize_lookup_key(stem)


def slugify_code(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def strip_trailing_units(value: str) -> str:
    cleaned = value.strip()
    while True:
        updated = re.sub(r"\s*(?:\(|\[)\s*[^()\[\]]+\s*(?:\)|\])\s*$", "", cleaned)
        if updated == cleaned:
            return cleaned.strip(" -_")
        cleaned = updated.strip()


def clean_asset_alias(value: str) -> str:
    cleaned = strip_trailing_units(clean_duplicate_suffix(value))
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"^\s*3\s*ph\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+-\s+", " - ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_")
    return cleaned


@dataclass(frozen=True)
class PlantSeed:
    id_impianto: int
    nome: str
    descrizione: str


@dataclass(frozen=True)
class SalaSeed:
    id_sala: int
    id_impianto: int
    codice: str
    nome: str
    abilitato: int


@dataclass(frozen=True)
class RemoteCsvFile:
    display_name: str
    source_url: str
    source_folder_url: str
    target_day: date | None = None
    last_modified: datetime | None = None
    bytes_read: int | None = None


@dataclass
class FileRuntimeState:
    last_http_last_modified: datetime | None = None
    last_bytes_read: int | None = None
    last_checked_at: datetime | None = None


@dataclass(frozen=True)
class CompressorIdentity:
    raw_alias: str
    normalized_name: str
    codice: str
    tipologia: str


@dataclass
class ParsedSalaRecord:
    timestamp: datetime
    metrics: dict[str, Any]


@dataclass
class ParsedCompressoreRecord:
    timestamp: datetime
    identity: CompressorIdentity
    metrics: dict[str, float | None]


@dataclass
class ParseResult:
    rows_seen: int = 0
    rows_valid_timestamp: int = 0
    rows_invalid_timestamp: int = 0
    timestamp_column: str | None = None
    sala_column_map: dict[str, str] = field(default_factory=dict)
    compressore_column_map: dict[str, tuple[str, CompressorIdentity]] = field(default_factory=dict)
    dynamic_sala_column_map: dict[str, str] = field(default_factory=dict)
    dynamic_sala_column_types: dict[str, str] = field(default_factory=dict)
    unrecognized_columns: list[str] = field(default_factory=list)
    sala_records: list[ParsedSalaRecord] = field(default_factory=list)
    compressore_records: list[ParsedCompressoreRecord] = field(default_factory=list)

    @property
    def max_timestamp(self) -> datetime | None:
        values = [item.timestamp for item in self.sala_records]
        values.extend(item.timestamp for item in self.compressore_records)
        return max(values) if values else None

    @property
    def min_timestamp(self) -> datetime | None:
        values = [item.timestamp for item in self.sala_records]
        values.extend(item.timestamp for item in self.compressore_records)
        return min(values) if values else None


@dataclass
class ImportOutcome:
    source: RemoteCsvFile
    mode: str
    sala_code: str
    rows_seen: int = 0
    rows_valid_timestamp: int = 0
    sala_rows_inserted: int = 0
    compressore_rows_inserted: int = 0
    compressori_missing: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    storico_write_started_at: datetime | None = None
    storico_written_at: datetime | None = None
    stato_sala_updated_at: datetime | None = None
    stato_compressori_updated_at: datetime | None = None
    current_sala_upserted: bool = False
    current_compressori_upserted: int = 0
    aggregate_range_start: datetime | None = None
    aggregate_range_end: datetime | None = None


PLANT_SEEDS: tuple[PlantSeed, ...] = (
    PlantSeed(1, "San Salvo", "Impianto San Salvo"),
    PlantSeed(2, "Marghera", "Impianto Marghera"),
)

SALA_SEEDS: tuple[SalaSeed, ...] = (
    SalaSeed(1, 1, "SS1", "SS1", 1),
    SalaSeed(2, 1, "SS2", "SS2", 1),
    SalaSeed(3, 1, "BRAVO", "Bravo", 1),
    SalaSeed(4, 1, "CENTAC", "Centac", 1),
    SalaSeed(5, 1, "LAMINATI", "Laminati", 1),
    SalaSeed(6, 1, "PRIMO_ALTA", "Primo alta", 1),
    SalaSeed(7, 1, "PRIMO_BASSA", "Primo bassa", 1),
    SalaSeed(8, 1, "SS1_COMP", "SS1 composizione", 1),
    SalaSeed(9, 1, "SS2_COMP", "SS2 composizione", 1),
    SalaSeed(10, 2, "LAM_MP_7BAR", "Laminati + Materie Prime 7 BAR", 1),
    SalaSeed(11, 2, "LAM_ALTA", "Laminati alta", 1),
    SalaSeed(12, 2, "TAGLIERIA", "Taglieria", 1),
    SalaSeed(13, 2, "COMP_BP", "Composizione Bassa Pressione", 1),
    SalaSeed(14, 2, "FORNO_EF", "Forno + Elettrofiltro", 1),
    SalaSeed(15, 1, "CRS_BASSA", "CRSBassa", 0),
    SalaSeed(16, 1, "CRS_ALTA", "CRSAlta", 0),
)

RAW_FILE_SALA_CODE_MAP: dict[str, str] = {
    "SS1.csv": "SS1",
    "SS2.csv": "SS2",
    "BRAVO.csv": "BRAVO",
    "CENTAC.csv": "CENTAC",
    "LAMINATO.csv": "LAMINATI",
    "PRIMOAlta.csv": "PRIMO_ALTA",
    "PRIMOBassa.csv": "PRIMO_BASSA",
    "LaminatiAlta.csv": "LAM_ALTA",
    "Taglieria.csv": "TAGLIERIA",
    "Forno.csv": "FORNO_EF",
    "COMPOSIZIONE.csv": "SS1_COMP",
    "SS2 Bassa Pressione.csv": "SS2_COMP",
    "MateriePrime.csv": "COMP_BP",
    "LaminatiBassa.csv": "LAM_MP_7BAR",
}

FILE_SALA_CODE_MAP: dict[str, str] = {
    normalize_source_filename(key): value
    for key, value in RAW_FILE_SALA_CODE_MAP.items()
}

SALA_METRIC_SYNONYMS: dict[str, set[str]] = {
    "flusso": {"flusso m3 h"},
    "flusso_tot": {"flusso tot m3 h", "flusso total m3 h"},
    "flusso_7barg": {"flusso 7 barg m3 h"},
    "potAttTotale": {"potenza attiva tot kw"},
    "consSpecifico": {"consumo specifico kwh m3", "consumo specifico kwh m 3"},
    "pressione": {"pressione bar"},
    "pressione2": {"pressione bar 2"},
    "temperatura": {"temperatura c"},
    "temperatura2": {"temperatura c 2"},
    "dewpoint": {"dew point c", "dew point"},
    "dewpoint_td": {"dew point ctd", "dew point td", "dew point c td"},
    "umidita_relativa": {"relative humidity", "umidita relativa"},
    "totMetriCubi": {"totale m3", "totale m 3"},
}

COMPRESSORE_PREFIX_MAP: tuple[tuple[str, str], ...] = (
    ("energia attiva totale", "energiaAttivaTotale"),
    ("potenza attiva", "potAttiva"),
    ("cosphi", "cosphi"),
    ("u1", "u1"),
    ("u2", "u2"),
    ("u3", "u3"),
    ("i1", "l1"),
    ("i2", "l2"),
    ("i3", "l3"),
    ("l1", "l1"),
    ("l2", "l2"),
    ("l3", "l3"),
)

EXPECTED_TABLES = (
    "impianti",
    "sale",
    "compressori",
    "registrazioni_sale",
    "registrazioni_compressori",
    "stato_sale_corrente",
    "stato_compressori_corrente",
)


@dataclass
class AppConfig:
    base_csv_url: str
    db_url: str
    poll_seconds: int
    run_once: bool
    replace_existing: bool
    status_file: str
    log_level: str
    target_date: date | None
    from_date: date | None
    to_date: date | None


class SchemaBundle:
    def __init__(self, engine: Engine):
        self._ensure_current_state_tables(engine)
        metadata = MetaData()
        metadata.reflect(bind=engine, only=list(EXPECTED_TABLES))
        self.metadata = metadata
        missing = [name for name in EXPECTED_TABLES if name not in metadata.tables]
        if missing:
            raise RuntimeError(f"Missing required tables in target DB: {', '.join(missing)}")

        self.impianti = metadata.tables["impianti"]
        self.sale = metadata.tables["sale"]
        self.compressori = metadata.tables["compressori"]
        self.registrazioni_sale = metadata.tables["registrazioni_sale"]
        self.registrazioni_compressori = metadata.tables["registrazioni_compressori"]
        self.stato_sale_corrente = metadata.tables["stato_sale_corrente"]
        self.stato_compressori_corrente = metadata.tables["stato_compressori_corrente"]
        self.inspector = inspect(engine)

    @staticmethod
    def pk_name(table: Table) -> str:
        columns = list(table.primary_key.columns)
        if not columns:
            raise RuntimeError(f"Table {table.name} has no primary key")
        return columns[0].name

    @staticmethod
    def _ensure_current_state_tables(engine: Engine) -> None:
        metadata = MetaData()
        stato_sale_corrente = Table(
            "stato_sale_corrente",
            metadata,
            Column("idSala", Integer, primary_key=True),
            Column("timestamp", DateTime, nullable=True),
            Column("pressione", Numeric(18, 4), nullable=True),
            Column("pressione2", Numeric(18, 4), nullable=True),
            Column("temperatura", Numeric(18, 4), nullable=True),
            Column("temperatura2", Numeric(18, 4), nullable=True),
            Column("dewpoint", Numeric(18, 4), nullable=True),
            Column("dewpoint_td", Numeric(18, 4), nullable=True),
            Column("umidita_relativa", Numeric(18, 4), nullable=True),
            Column("flusso", Numeric(18, 4), nullable=True),
            Column("flusso_tot", Numeric(18, 4), nullable=True),
            Column("flusso_7barg", Numeric(18, 4), nullable=True),
            Column("potAttTotale", Numeric(18, 4), nullable=True),
            Column("consSpecifico", Numeric(18, 4), nullable=True),
            Column("totMetriCubi", Numeric(18, 4), nullable=True),
            Column("statoDati", Integer, nullable=True),
            Column("updated_at", TIMESTAMP, nullable=False, server_default=sql_text("CURRENT_TIMESTAMP")),
        )
        stato_compressori_corrente = Table(
            "stato_compressori_corrente",
            metadata,
            Column("idCompressore", Integer, primary_key=True),
            Column("idSala", Integer, nullable=True),
            Column("timestamp", DateTime, nullable=True),
            Column("potAttiva", Numeric(18, 4), nullable=True),
            Column("u1", Numeric(18, 4), nullable=True),
            Column("u2", Numeric(18, 4), nullable=True),
            Column("u3", Numeric(18, 4), nullable=True),
            Column("l1", Numeric(18, 4), nullable=True),
            Column("l2", Numeric(18, 4), nullable=True),
            Column("l3", Numeric(18, 4), nullable=True),
            Column("cosphi", Numeric(18, 4), nullable=True),
            Column("energiaAttivaTotale", Numeric(18, 4), nullable=True),
            Column("statoCompressore", Integer, nullable=True),
            Column("statoDati", Integer, nullable=True),
            Column("updated_at", TIMESTAMP, nullable=False, server_default=sql_text("CURRENT_TIMESTAMP")),
        )
        metadata.create_all(bind=engine, checkfirst=True)


class DatabaseAdapter:
    def __init__(self, session_factory: sessionmaker, schema: SchemaBundle):
        self.session_factory = session_factory
        self.schema = schema
        self.engine = session_factory.kw["bind"]
        self._ensure_sale_aggregate_secondary_columns()
        try:
            for warning in validate_rollup_schema(self.engine):
                LOGGER.warning("%s", warning)
        except Exception as exc:
            LOGGER.warning("Aggregate rollup schema validation failed during ingestor startup: %s", exc)

    def _ensure_sale_aggregate_secondary_columns(self) -> None:
        aggregate_tables = [
            level_spec_for_granularity("sale", granularity).target_table
            for granularity in ("1min", "15min", "1h", "1d", "1month")
        ]
        existing_tables = set(inspect(self.engine).get_table_names())
        if not any(table_name in existing_tables for table_name in aggregate_tables):
            return

        altered_columns: dict[str, list[str]] = {}
        try:
            altered_columns = ensure_sale_aggregate_secondary_columns(self.engine)
        except SQLAlchemyError as exc:
            LOGGER.warning(
                "Cannot ensure secondary columns on sale aggregate tables during ingestor startup: %s",
                exc,
            )

        if altered_columns:
            LOGGER.info(
                "Added secondary sale aggregate columns: %s",
                ", ".join(f"{table}({', '.join(columns)})" for table, columns in altered_columns.items()),
            )

    def ensure_reference_data(self) -> None:
        with self.session_factory.begin() as session:
            for impianto in PLANT_SEEDS:
                payload = self._table_payload(
                    self.schema.impianti,
                    {
                        "idImpianto": impianto.id_impianto,
                        "nome": impianto.nome,
                        "descrizione": impianto.descrizione,
                    },
                )
                self._upsert_by_pk(session, self.schema.impianti, payload)

            for sala in SALA_SEEDS:
                payload = self._table_payload(
                    self.schema.sale,
                    {
                        "idSala": sala.id_sala,
                        "idImpianto": sala.id_impianto,
                        "codice": sala.codice,
                        "nome": sala.nome,
                        "abilitato": sala.abilitato,
                    },
                )
                self._upsert_by_pk(session, self.schema.sale, payload)

    def resolve_sala_id(self, sala_code: str) -> int | None:
        with self.session_factory() as session:
            stmt = select(self.schema.sale.c.idSala).where(func.upper(self.schema.sale.c.codice) == sala_code.upper())
            return session.execute(stmt).scalar_one_or_none()

    def get_last_timestamp(self, sala_id: int) -> datetime | None:
        with self.session_factory() as session:
            stmt = select(func.max(self.schema.registrazioni_sale.c.timestamp)).where(
                self.schema.registrazioni_sale.c.idSala == sala_id
            )
            return session.execute(stmt).scalar_one_or_none()

    def get_last_timestamp_for_day(self, sala_id: int, target_day: date) -> datetime | None:
        day_start = datetime.combine(target_day, datetime.min.time())
        day_end = day_start + timedelta(days=1)
        with self.session_factory() as session:
            stmt = select(func.max(self.schema.registrazioni_sale.c.timestamp)).where(
                and_(
                    self.schema.registrazioni_sale.c.idSala == sala_id,
                    self.schema.registrazioni_sale.c.timestamp >= day_start,
                    self.schema.registrazioni_sale.c.timestamp < day_end,
                )
            )
            return session.execute(stmt).scalar_one_or_none()

    def ensure_dynamic_room_columns(self, parse_result: ParseResult) -> dict[str, str]:
        if not parse_result.dynamic_sala_column_map:
            return {}

        added: dict[str, str] = {}
        history_columns = set(self.schema.registrazioni_sale.c.keys())
        current_columns = set(self.schema.stato_sale_corrente.c.keys())
        failed: dict[str, str] = {}
        with self.engine.begin() as connection:
            for source_column, db_column in parse_result.dynamic_sala_column_map.items():
                sql_type = parse_result.dynamic_sala_column_types[db_column]
                try:
                    if db_column not in history_columns:
                        connection.execute(
                            sql_text(
                                f"ALTER TABLE `{self.schema.registrazioni_sale.name}` "
                                f"ADD COLUMN `{db_column}` {sql_type} NULL"
                            )
                        )
                        history_columns.add(db_column)
                    if db_column not in current_columns:
                        connection.execute(
                            sql_text(
                                f"ALTER TABLE `{self.schema.stato_sale_corrente.name}` "
                                f"ADD COLUMN `{db_column}` {sql_type} NULL"
                            )
                        )
                        current_columns.add(db_column)
                    added[source_column] = db_column
                except SQLAlchemyError as exc:
                    failed[source_column] = db_column
                    LOGGER.warning(
                        "Cannot add DB column %s for CSV column %r on room tables: %s. Continuing without that column.",
                        db_column,
                        source_column,
                        exc,
                    )

        if added:
            self.schema = SchemaBundle(self.engine)
            LOGGER.warning("Added new DB columns to registrazioni_sale/stato_sale_corrente: %s", added)
        if failed:
            parse_result.dynamic_sala_column_map = {
                source_column: db_column
                for source_column, db_column in parse_result.dynamic_sala_column_map.items()
                if source_column not in failed
            }
            parse_result.dynamic_sala_column_types = {
                db_column: sql_type
                for db_column, sql_type in parse_result.dynamic_sala_column_types.items()
                if db_column not in failed.values()
            }
        return added

    def resolve_compressore_id(self, session: Session, sala_id: int, identity: CompressorIdentity) -> int | None:
        table = self.schema.compressori
        identity_conditions = []
        if "codifica" in table.c:
            identity_conditions.append(func.upper(table.c.codifica) == identity.codice.upper())
        if "codice" in table.c:
            identity_conditions.append(func.upper(table.c.codice) == identity.codice.upper())
        if "nome" in table.c:
            identity_conditions.append(func.upper(table.c.nome) == identity.normalized_name.upper())
        if identity_conditions:
            stmt = select(table.c.idCompressore).where(
                and_(
                    table.c.idSala == sala_id,
                    or_(*identity_conditions),
                )
            )
            existing = session.execute(stmt).scalar_one_or_none()
            if existing is not None:
                return int(existing)
        return None

    def insert_parse_result(
        self,
        source: RemoteCsvFile,
        mode: str,
        sala_id: int,
        sala_code: str,
        parse_result: ParseResult,
        *,
        replace_existing: bool = False,
    ) -> ImportOutcome:
        sala_rows: list[dict[str, Any]] = []
        compressore_rows: list[dict[str, Any]] = []
        current_sala_row: dict[str, Any] | None = None
        current_compressore_rows: dict[int, dict[str, Any]] = {}
        compressori_missing: list[str] = []
        storico_write_started_at: datetime | None = None
        storico_written_at: datetime | None = None
        stato_sala_updated_at: datetime | None = None
        stato_compressori_updated_at: datetime | None = None
        current_sala_upserted = False
        current_compressori_upserted = 0

        self.ensure_dynamic_room_columns(parse_result)

        with self.session_factory.begin() as session:
            compressore_ids: dict[str, int] = {}
            for record in parse_result.compressore_records:
                if record.identity.codice in compressore_ids:
                    continue
                compressore_id = self.resolve_compressore_id(session, sala_id, record.identity)
                if compressore_id is None:
                    missing_name = record.identity.normalized_name
                    if missing_name not in compressori_missing:
                        compressori_missing.append(missing_name)
                    continue
                compressore_ids[record.identity.codice] = compressore_id

            if replace_existing:
                self._delete_existing_range(
                    session,
                    sala_id=sala_id,
                    compressore_ids=list(compressore_ids.values()),
                    parse_result=parse_result,
                )

            for record in parse_result.sala_records:
                payload = self._table_payload(
                    self.schema.registrazioni_sale,
                    {
                        "idSala": sala_id,
                        "timestamp": record.timestamp,
                        "statoDati": DEFAULT_STATO_DATI,
                        "created_at": utcnow_naive(),
                        **record.metrics,
                    },
                    keep_none=True,
                )
                if payload:
                    sala_rows.append(payload)
                if current_sala_row is None or record.timestamp >= current_sala_row["timestamp"]:
                    current_payload = self._table_payload(
                        self.schema.stato_sale_corrente,
                        {
                            "idSala": sala_id,
                            "timestamp": record.timestamp,
                            "statoDati": DEFAULT_STATO_DATI,
                            "updated_at": utcnow_naive(),
                            **record.metrics,
                        },
                        keep_none=True,
                    )
                    if current_payload:
                        current_sala_row = current_payload

            for record in parse_result.compressore_records:
                compressore_id = compressore_ids.get(record.identity.codice)
                if compressore_id is None:
                    continue
                payload = self._table_payload(
                    self.schema.registrazioni_compressori,
                    {
                        "idCompressore": compressore_id,
                        "idSala": sala_id,
                        "timestamp": record.timestamp,
                        "statoDati": DEFAULT_STATO_DATI,
                        "created_at": utcnow_naive(),
                        **record.metrics,
                    },
                    keep_none=True,
                )
                if payload:
                    compressore_rows.append(payload)
                current_payload = self._table_payload(
                    self.schema.stato_compressori_corrente,
                    {
                        "idCompressore": compressore_id,
                        "idSala": sala_id,
                        "timestamp": record.timestamp,
                        "statoDati": DEFAULT_STATO_DATI,
                        "updated_at": utcnow_naive(),
                        **record.metrics,
                    },
                    keep_none=True,
                )
                existing_current = current_compressore_rows.get(compressore_id)
                if current_payload and (
                    existing_current is None or record.timestamp >= existing_current["timestamp"]
                ):
                    current_compressore_rows[compressore_id] = current_payload

            LOGGER.debug("Prepared sala rows: %s", len(sala_rows))
            LOGGER.debug("Prepared compressore rows: %s", len(compressore_rows))
            LOGGER.debug("Sample sala payload: %s", sala_rows[0] if sala_rows else None)
            LOGGER.debug("Sample compressore payload: %s", compressore_rows[0] if compressore_rows else None)

            storico_write_started_at = utcnow_naive()
            sala_inserted = self._bulk_insert(session, self.schema.registrazioni_sale, sala_rows)
            compressore_inserted = self._bulk_insert(session, self.schema.registrazioni_compressori, compressore_rows)
            storico_written_at = utcnow_naive()
            if current_sala_row:
                current_sala_upserted = self._upsert_current_state_if_newer(
                    session,
                    self.schema.stato_sale_corrente,
                    current_sala_row,
                )
                stato_sala_updated_at = utcnow_naive()
            for payload in current_compressore_rows.values():
                if self._upsert_current_state_if_newer(session, self.schema.stato_compressori_corrente, payload):
                    current_compressori_upserted += 1
            if current_compressore_rows:
                stato_compressori_updated_at = utcnow_naive()

        return ImportOutcome(
            source=source,
            mode=mode,
            sala_code=sala_code,
            rows_seen=parse_result.rows_seen,
            rows_valid_timestamp=parse_result.rows_valid_timestamp,
            sala_rows_inserted=sala_inserted,
            compressore_rows_inserted=compressore_inserted,
            compressori_missing=compressori_missing,
            warnings=[],
            storico_write_started_at=storico_write_started_at,
            storico_written_at=storico_written_at,
            stato_sala_updated_at=stato_sala_updated_at,
            stato_compressori_updated_at=stato_compressori_updated_at,
            current_sala_upserted=current_sala_upserted,
            current_compressori_upserted=current_compressori_upserted,
            aggregate_range_start=parse_result.min_timestamp,
            aggregate_range_end=(parse_result.max_timestamp + timedelta(microseconds=1)) if parse_result.max_timestamp else None,
        )

    def _delete_existing_range(
        self,
        session: Session,
        *,
        sala_id: int,
        compressore_ids: list[int],
        parse_result: ParseResult,
    ) -> None:
        min_ts = parse_result.min_timestamp
        max_ts = parse_result.max_timestamp
        if min_ts is None or max_ts is None:
            return

        sala_conditions = and_(
            self.schema.registrazioni_sale.c.idSala == sala_id,
            self.schema.registrazioni_sale.c.timestamp >= min_ts,
            self.schema.registrazioni_sale.c.timestamp <= max_ts,
        )
        sala_existing = session.execute(
            select(func.count())
            .select_from(self.schema.registrazioni_sale)
            .where(sala_conditions)
        ).scalar_one()
        if sala_existing:
            sala_stmt = delete(self.schema.registrazioni_sale).where(sala_conditions)
            session.execute(sala_stmt)

        if compressore_ids:
            compressori_conditions = and_(
                self.schema.registrazioni_compressori.c.idCompressore.in_(compressore_ids),
                self.schema.registrazioni_compressori.c.timestamp >= min_ts,
                self.schema.registrazioni_compressori.c.timestamp <= max_ts,
            )
            compressori_existing = session.execute(
                select(func.count())
                .select_from(self.schema.registrazioni_compressori)
                .where(compressori_conditions)
            ).scalar_one()
            if compressori_existing:
                compressori_stmt = delete(self.schema.registrazioni_compressori).where(compressori_conditions)
                session.execute(compressori_stmt)

    def _bulk_insert(self, session: Session, table: Table, rows: list[dict[str, Any]], chunk_size: int = 500) -> int:
        if not rows:
            return 0

        inserted = 0
        buckets: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for row in rows:
            key = tuple(sorted(row.keys()))
            buckets.setdefault(key, []).append(row)

        for shape, bucket_rows in buckets.items():
            LOGGER.debug("Bulk insert table=%s shape=%s rows=%s", table.name, list(shape), len(bucket_rows))
            for offset in range(0, len(bucket_rows), chunk_size):
                chunk = bucket_rows[offset: offset + chunk_size]
                result = session.execute(table.insert(), chunk)
                inserted += int(result.rowcount or 0)
        return inserted

    def _upsert_by_pk(self, session: Session, table: Table, payload: dict[str, Any]) -> None:
        pk_name = self.schema.pk_name(table)
        dialect_name = (session.bind.dialect.name if session.bind is not None else self.engine.dialect.name).lower()
        if dialect_name == "mysql":
            stmt = mysql_insert(table).values(**payload)
            update_cols = {key: stmt.inserted[key] for key in payload if key != pk_name}
            session.execute(stmt.on_duplicate_key_update(**update_cols))
            return
        if dialect_name == "sqlite":
            stmt = sqlite_insert(table).values(**payload)
            update_cols = {key: stmt.excluded[key] for key in payload if key != pk_name}
            session.execute(stmt.on_conflict_do_update(index_elements=[pk_name], set_=update_cols))
            return
        self._update_or_insert(session, table, payload)

    def _upsert_current_state_if_newer(self, session: Session, table: Table, payload: dict[str, Any]) -> bool:
        pk_name = self.schema.pk_name(table)
        pk_column = table.c[pk_name]
        existing_row = session.execute(
            select(pk_column, table.c.timestamp).where(pk_column == payload[pk_name])
        ).first()
        if existing_row is not None:
            existing_ts = existing_row._mapping.get("timestamp")
            incoming_ts = payload.get("timestamp")
            if existing_ts is not None and incoming_ts is not None and incoming_ts < existing_ts:
                LOGGER.debug(
                    "Skipping stale current-state update table=%s pk=%s existing_ts=%s incoming_ts=%s",
                    table.name,
                    payload[pk_name],
                    existing_ts,
                    incoming_ts,
                )
                return False
        self._update_or_insert(session, table, payload)
        return True

    @staticmethod
    def _update_or_insert(session: Session, table: Table, payload: dict[str, Any]) -> None:
        pk_name = next(iter(table.primary_key.columns)).name
        pk_column = table.c[pk_name]
        existing = session.execute(select(pk_column).where(pk_column == payload[pk_name])).first()
        if existing is not None:
            update_payload = {key: value for key, value in payload.items() if key != pk_name}
            if update_payload:
                session.execute(table.update().where(pk_column == payload[pk_name]).values(**update_payload))
            return
        session.execute(table.insert().values(**payload))

    @staticmethod
    def _table_payload(table: Table, payload: dict[str, Any], keep_none: bool = False) -> dict[str, Any]:
        filtered = {
            key: value
            for key, value in payload.items()
            if key in table.c and (keep_none or value is not None)
        }
        dropped = sorted(set(payload.keys()) - set(filtered.keys()))
        if dropped:
            LOGGER.debug("Dropped keys for %s: %s", table.name, dropped)
        return filtered


def parse_http_last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC).replace(tzinfo=None)
    return parsed.astimezone(UTC).replace(tzinfo=None)


def clean_href(raw_href: str) -> str:
    return unquote(raw_href.split("?", 1)[0].split("#", 1)[0]).strip()


def build_day_source_url(base_csv_url: str, target_day: date) -> str:
    return f"{base_csv_url.rstrip('/')}/{target_day:%Y/%m/%d}"


def list_remote_links(directory_url: str) -> list[str]:
    response = requests.get(f"{directory_url.rstrip('/')}/", timeout=REMOTE_TIMEOUT_SECONDS)
    response.raise_for_status()
    hrefs = HREF_RE.findall(response.text)
    return [clean_href(item) for item in hrefs if clean_href(item)]


def list_remote_csv_files(directory_url: str) -> list[RemoteCsvFile]:
    discovered: list[RemoteCsvFile] = []
    for href in list_remote_links(directory_url):
        if href in {"../", "..", "/"} or href.endswith("/"):
            continue
        absolute = urljoin(f"{directory_url.rstrip('/')}/", href)
        absolute = absolute.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        basename = absolute.rsplit("/", 1)[-1]
        if not basename.lower().endswith(".csv"):
            continue
        discovered.append(
            RemoteCsvFile(
                display_name=basename,
                source_url=absolute,
                source_folder_url=directory_url.rstrip("/"),
            )
        )
    return sorted(discovered, key=lambda item: item.source_url.lower())


def get_remote_last_modified(source_url: str) -> datetime | None:
    try:
        response = requests.head(source_url, timeout=REMOTE_TIMEOUT_SECONDS, allow_redirects=True)
        response.raise_for_status()
    except Exception:
        return None
    return parse_http_last_modified(response.headers.get("Last-Modified"))


def fetch_remote_dataframe(
    source: RemoteCsvFile,
    *,
    log_details: bool = True,
) -> tuple[pd.DataFrame, datetime | None, int]:
    response = requests.get(source.source_url, timeout=REMOTE_TIMEOUT_SECONDS)
    response.raise_for_status()
    last_modified = parse_http_last_modified(response.headers.get("Last-Modified"))
    bytes_read = len(response.content)

    read_attempts = (
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "cp1252"},
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "utf-8"},
    )

    last_error: Exception | None = None
    for options in read_attempts:
        try:
            text = response.content.decode(options["encoding"])
            frame = pd.read_csv(
                io.StringIO(text),
                dtype=str,
                keep_default_na=False,
                on_bad_lines="skip",
                sep=options["sep"],
            )
            return frame, last_modified, bytes_read
        except Exception as exc:
            last_error = exc

    raise ValueError(f"Could not read remote CSV {source.display_name}: {last_error}")


def map_sala_metric(header: str) -> str | None:
    key = normalize_lookup_key(header)
    for field_name, synonyms in SALA_METRIC_SYNONYMS.items():
        if key in synonyms:
            return field_name
    return None


def canonicalize_compressore(raw_alias: str) -> CompressorIdentity:
    normalized_name = clean_asset_alias(raw_alias)
    lookup = normalize_lookup_key(normalized_name)
    if "ess" in lookup:
        tipologia = "dryer"
    elif "booster" in lookup:
        tipologia = "booster"
    else:
        tipologia = "compressore"
    return CompressorIdentity(
        raw_alias=normalized_name,
        normalized_name=normalized_name,
        codice=slugify_code(normalized_name),
        tipologia=tipologia,
    )


def parse_compressore_header(header: str) -> tuple[str, CompressorIdentity] | None:
    cleaned = clean_duplicate_suffix(header)
    collapsed = re.sub(r"\s+", " ", cleaned).strip()
    for prefix, field_name in COMPRESSORE_PREFIX_MAP:
        match = re.match(rf"^{re.escape(prefix)}\s+(.+)$", collapsed, flags=re.IGNORECASE)
        if not match:
            continue
        raw_alias = clean_asset_alias(match.group(1))
        if not raw_alias:
            return None
        identity = canonicalize_compressore(raw_alias)
        if VERBOSE_COLUMN_DEBUG:
            LOGGER.debug(
                "COMPRESSORE_HEADER raw=%r field=%r compressore=%r codice=%r",
                header,
                field_name,
                identity.normalized_name,
                identity.codice,
            )
        return field_name, identity
    return None


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    text = text.replace("\xa0", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def dynamic_column_name(header: str) -> str:
    normalized = normalize_lookup_key(header)
    slug = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    slug = re.sub(r"_+", "_", slug)
    if not slug:
        slug = "value"
    if not slug[0].isalpha():
        slug = f"col_{slug}"
    return f"csv_{slug}"[:64]


def detect_dynamic_column_sql_type(series: pd.Series) -> str:
    has_values = False
    for value in series:
        text = str(value).strip() if value is not None else ""
        if not text or text.lower() in {"nan", "none", "null"}:
            continue
        has_values = True
        if parse_float(value) is None:
            return "TEXT"
    return "DOUBLE" if has_values else "TEXT"


def parse_dynamic_value(value: Any, sql_type: str) -> float | str | None:
    if sql_type == "DOUBLE":
        return parse_float(value)

    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


class CsvParser:
    def parse_dataframe(
        self,
        source_name: str,
        frame: pd.DataFrame,
        since_ts: datetime | None,
        *,
        log_details: bool = True,
    ) -> ParseResult:
        frame = frame.dropna(axis=1, how="all")
        if frame.empty:
            return ParseResult()

        timestamp_column = self._detect_timestamp_column(frame)
        if timestamp_column is None:
            raise ValueError(f"Timestamp column not found in {source_name}")

        sala_column_map: dict[str, str] = {}
        compressore_column_map: dict[str, tuple[str, CompressorIdentity]] = {}
        dynamic_sala_column_map: dict[str, str] = {}
        dynamic_sala_column_types: dict[str, str] = {}
        unrecognized_columns: list[str] = []

        for column_name in frame.columns:
            if column_name == timestamp_column:
                continue
            sala_field = map_sala_metric(column_name)
            compressore_field = None
            if sala_field:
                sala_column_map[column_name] = sala_field
            else:
                compressore_field = parse_compressore_header(column_name)
                if compressore_field:
                    compressore_column_map[column_name] = compressore_field
                else:
                    db_column = dynamic_column_name(column_name)
                    dynamic_sala_column_map[column_name] = db_column
                    dynamic_sala_column_types[db_column] = detect_dynamic_column_sql_type(frame[column_name])
                    unrecognized_columns.append(column_name)

            if VERBOSE_COLUMN_DEBUG:
                LOGGER.debug(
                    "COLUMN raw=%r normalized=%r sala_metric=%r compressore_metric=%r",
                    column_name,
                    normalize_lookup_key(column_name),
                    sala_field,
                    compressore_field[0] if compressore_field else None,
                )

        result = ParseResult(
            rows_seen=len(frame.index),
            timestamp_column=timestamp_column,
            sala_column_map=sala_column_map,
            compressore_column_map=compressore_column_map,
            dynamic_sala_column_map=dynamic_sala_column_map,
            dynamic_sala_column_types=dynamic_sala_column_types,
            unrecognized_columns=unrecognized_columns,
        )

        parsed_ts = pd.to_datetime(frame[timestamp_column], format="%d/%m/%Y %H:%M:%S", errors="coerce", dayfirst=True)
        for row_number, (_, row) in enumerate(frame.iterrows(), start=2):
            raw_ts = parsed_ts.iloc[row_number - 2]
            if pd.isna(raw_ts):
                result.rows_invalid_timestamp += 1
                LOGGER.warning("Skipping row in %s due to invalid timestamp at CSV row %s", source_name, row_number)
                continue

            event_time = raw_ts.to_pydatetime().replace(tzinfo=None)
            result.rows_valid_timestamp += 1
            if since_ts is not None and event_time <= since_ts:
                continue

            sala_metrics: dict[str, float | None] = {}
            per_compressore_metrics: dict[str, dict[str, float | None]] = {}
            per_compressore_identity: dict[str, CompressorIdentity] = {}

            for column_name, field_name in sala_column_map.items():
                sala_metrics[field_name] = parse_float(row[column_name])

            for column_name, db_column in dynamic_sala_column_map.items():
                sql_type = dynamic_sala_column_types[db_column]
                sala_metrics[db_column] = parse_dynamic_value(row[column_name], sql_type)

            for column_name, (field_name, identity) in compressore_column_map.items():
                per_compressore_metrics.setdefault(identity.codice, {})[field_name] = parse_float(row[column_name])
                per_compressore_identity[identity.codice] = identity

            if sala_column_map or dynamic_sala_column_map:
                result.sala_records.append(ParsedSalaRecord(timestamp=event_time, metrics=sala_metrics))

            for codice, metrics in per_compressore_metrics.items():
                result.compressore_records.append(
                    ParsedCompressoreRecord(
                        timestamp=event_time,
                        identity=per_compressore_identity[codice],
                        metrics=metrics,
                    )
                )

        return result

    @staticmethod
    def _detect_timestamp_column(frame: pd.DataFrame) -> str | None:
        candidate_names = [
            col
            for col in frame.columns
            if normalize_lookup_key(col) in {"timestamp", "data ora", "data", "date time", "dd mm yyyy hh mm ss"}
        ]
        candidate_names.extend([col for col in frame.columns if col not in candidate_names][:3])

        best_name: str | None = None
        best_valid = -1
        for column_name in candidate_names:
            parsed = pd.to_datetime(frame[column_name], format="%d/%m/%Y %H:%M:%S", errors="coerce", dayfirst=True)
            valid = int(parsed.notna().sum())
            if valid > best_valid:
                best_name = column_name
                best_valid = valid
        return best_name if best_valid > 0 else None


class CsvToDbIngestor:
    def __init__(self, config: AppConfig):
        self.config = config
        self.engine = create_engine(config.db_url, pool_pre_ping=True, future=True)
        self.source_tz = ZoneInfo(SOURCE_TIMEZONE)
        self._validate_target_database()
        self.schema = SchemaBundle(self.engine)
        self._log_schema()
        self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.db = DatabaseAdapter(self.session_factory, self.schema)
        self.parser = CsvParser()
        self.runtime_state: dict[str, FileRuntimeState] = {}
        self.db.ensure_reference_data()
        self._status_payload: dict[str, Any] = {
            "pid": os.getpid(),
            "running": False,
            "phase": "idle",
            "last_heartbeat": None,
            "current_file": None,
            "target_days": [],
            "sources_in_cycle": 0,
            "sources_processed_in_cycle": 0,
            "last_insert_at": None,
            "last_insert_file": None,
            "last_insert_sala": None,
            "last_insert_rows_sale": 0,
            "last_insert_rows_compressori": 0,
            "last_error": None,
        }

    def _write_status(self, **updates: Any) -> None:
        self._status_payload.update(updates)
        self._status_payload["last_heartbeat"] = utcnow_naive().isoformat()
        try:
            with open(self.config.status_file, "w", encoding="utf-8") as fh:
                json.dump(self._status_payload, fh, ensure_ascii=True, indent=2)
        except Exception:
            LOGGER.warning("Could not write status file %s", self.config.status_file)

    def run(self) -> None:
        self._write_status(running=True, phase="starting", last_error=None)
        try:
            if self.config.run_once:
                if self.config.from_date or self.config.to_date:
                    start_date = self.config.from_date or self.config.to_date
                    end_date = self.config.to_date or self.config.from_date
                    assert start_date is not None
                    assert end_date is not None
                    target_days = build_date_range(start_date, end_date)
                else:
                    target_day = self.config.target_date or (datetime.now(self.source_tz).date() - timedelta(days=1))
                    target_days = [target_day]
                self._write_status(phase="once")
                self.run_cycle(mode="once", target_days=target_days)
                return

            if self.config.from_date or self.config.to_date:
                start_date = self.config.from_date or self.config.to_date
                end_date = self.config.to_date or self.config.from_date
                assert start_date is not None
                assert end_date is not None
                backfill_days = build_date_range(start_date, end_date)
            else:
                initial_day = self.config.target_date or (datetime.now(self.source_tz).date() - timedelta(days=1))
                backfill_days = [initial_day]
            self._write_status(phase="backfill")
            self.run_cycle(mode="backfill", target_days=backfill_days)

            while True:
                self._write_status(phase="poll")
                time.sleep(self.config.poll_seconds)
                self.run_cycle(mode="poll", target_days=self._poll_target_days())
        finally:
            self._write_status(running=False, phase="stopped")

    def run_cycle(self, mode: str, target_days: list[date]) -> None:
        log_details = False
        self._write_status(
            running=True,
            phase=mode,
            last_error=None,
            current_file=None,
            target_days=[day.isoformat() for day in target_days],
            sources_in_cycle=0,
            sources_processed_in_cycle=0,
        )
        sources = self._discover_sources(target_days, log_details=log_details)
        self._write_status(sources_in_cycle=len(sources))
        for index, source in enumerate(sources, start=1):
            try:
                self._write_status(current_file=source.display_name, sources_processed_in_cycle=index - 1)
                self._process_source(source, mode=mode)
                self._write_status(current_file=source.display_name, sources_processed_in_cycle=index)
            except Exception:
                self._write_status(last_error=f"Failed processing {source.display_name}")
                LOGGER.exception("Failed processing %s", source.display_name)
        self._write_status(current_file=None)

    def _discover_sources(self, target_days: list[date], *, log_details: bool) -> list[RemoteCsvFile]:
        discovered: dict[str, RemoteCsvFile] = {}
        for target_day in target_days:
            source_url = build_day_source_url(self.config.base_csv_url, target_day)
            try:
                files = list_remote_csv_files(source_url)
            except Exception as exc:
                LOGGER.warning("Could not list CSV directory %s: %s", source_url, exc)
                continue
            for source in files:
                discovered[source.source_url] = RemoteCsvFile(
                    display_name=source.display_name,
                    source_url=source.source_url,
                    source_folder_url=source.source_folder_url,
                    target_day=target_day,
                    last_modified=source.last_modified,
                    bytes_read=source.bytes_read,
                )
        return sorted(discovered.values(), key=lambda item: item.source_url.lower())

    def _process_source(self, source: RemoteCsvFile, mode: str) -> None:
        log_details = False
        seen_at = utcnow_naive()
        sala_code = FILE_SALA_CODE_MAP.get(normalize_source_filename(source.display_name))
        if sala_code is None:
            LOGGER.warning("Skipping unmapped file %s", source.display_name)
            return

        sala_id = self.db.resolve_sala_id(sala_code)
        if sala_id is None:
            LOGGER.warning("Mapped file %s to sala codice %s, but sala not found in DB", source.display_name, sala_code)
            return

        log_pipeline(
            source.display_name,
            sala_code,
            event="seen",
            mode=mode,
            seen_at=seen_at,
            source_url=source.source_url,
        )

        head_checked_at = utcnow_naive()
        source_last_modified = get_remote_last_modified(source.source_url)
        source = RemoteCsvFile(
            display_name=source.display_name,
            source_url=source.source_url,
            source_folder_url=source.source_folder_url,
            target_day=source.target_day,
            last_modified=source_last_modified,
            bytes_read=source.bytes_read,
        )
        log_pipeline(
            source.display_name,
            sala_code,
            event="head",
            head_checked_at=head_checked_at,
            http_last_modified=source.last_modified,
        )

        should_fetch = True
        fetch_reason = "non_poll_mode"
        if mode == "poll":
            should_fetch, fetch_reason = self._should_fetch_source(source, seen_at)
        log_pipeline(
            source.display_name,
            sala_code,
            event="poll_decision",
            should_fetch=should_fetch,
            fetch_reason=fetch_reason,
            source_recheck_seconds=SOURCE_RECHECK_SECONDS,
        )
        if not should_fetch:
            LOGGER.debug("Skipping unchanged file %s", source.display_name)
            self._remember_source_state(source, checked_at=seen_at)
            return

        since_ts = None
        if not self.config.replace_existing:
            if source.target_day is not None:
                since_ts = self.db.get_last_timestamp_for_day(sala_id, source.target_day)
            else:
                since_ts = self.db.get_last_timestamp(sala_id)
        log_pipeline(
            source.display_name,
            sala_code,
            event="db_checkpoint",
            db_last_timestamp_before_import=since_ts,
        )

        fetch_started_at = utcnow_naive()
        frame, body_last_modified, bytes_read = fetch_remote_dataframe(source, log_details=log_details)
        fetch_finished_at = utcnow_naive()
        source = RemoteCsvFile(
            display_name=source.display_name,
            source_url=source.source_url,
            source_folder_url=source.source_folder_url,
            target_day=source.target_day,
            last_modified=body_last_modified or source.last_modified,
            bytes_read=bytes_read,
        )
        log_pipeline(
            source.display_name,
            sala_code,
            event="fetched",
            fetch_started_at=fetch_started_at,
            fetch_finished_at=fetch_finished_at,
            fetch_duration_ms=round((fetch_finished_at - fetch_started_at).total_seconds() * 1000, 2),
            body_last_modified=body_last_modified,
            bytes_read=bytes_read,
        )

        parse_started_at = utcnow_naive()
        parse_result = self.parser.parse_dataframe(source.display_name, frame, since_ts, log_details=log_details)
        parse_finished_at = utcnow_naive()
        csv_max_timestamp = parse_result.max_timestamp
        csv_min_timestamp = parse_result.min_timestamp
        log_pipeline(
            source.display_name,
            sala_code,
            event="parsed",
            parse_started_at=parse_started_at,
            parse_finished_at=parse_finished_at,
            parse_duration_ms=round((parse_finished_at - parse_started_at).total_seconds() * 1000, 2),
            csv_min_timestamp=csv_min_timestamp,
            csv_max_timestamp=csv_max_timestamp,
            rows_seen=parse_result.rows_seen,
            rows_valid_timestamp=parse_result.rows_valid_timestamp,
            rows_invalid_timestamp=parse_result.rows_invalid_timestamp,
        )
        if parse_result.unrecognized_columns:
            LOGGER.warning(
                "Extra room metrics found but not mapped to dedicated DB columns in %s: %s",
                source.display_name,
                parse_result.unrecognized_columns,
            )
        if not parse_result.sala_records and not parse_result.compressore_records:
            LOGGER.warning("No rows inserted for %s: no new valid records after filtering", source.display_name)
            log_pipeline(
                source.display_name,
                sala_code,
                event="no_new_rows",
                csv_max_timestamp=csv_max_timestamp,
                lag_csv_to_now_seconds=seconds_between(utcnow_naive(), csv_max_timestamp),
            )
            self._remember_source_state(source, checked_at=seen_at)
            return

        outcome = self.db.insert_parse_result(
            source=source,
            mode=mode,
            sala_id=sala_id,
            sala_code=sala_code,
            parse_result=parse_result,
            replace_existing=self.config.replace_existing,
        )
        db_updated_at = outcome.stato_compressori_updated_at or outcome.stato_sala_updated_at or outcome.storico_written_at
        lag_csv_to_db_seconds = seconds_between(db_updated_at, csv_max_timestamp)
        lag_csv_to_now_seconds = seconds_between(utcnow_naive(), csv_max_timestamp)
        log_pipeline(
            source.display_name,
            sala_code,
            event="historical_write",
            storico_write_started_at=outcome.storico_write_started_at,
            storico_written_at=outcome.storico_written_at,
            sala_rows_inserted=outcome.sala_rows_inserted,
            compressore_rows_inserted=outcome.compressore_rows_inserted,
        )
        log_pipeline(
            source.display_name,
            sala_code,
            event="current_state_write",
            stato_sala_updated_at=outcome.stato_sala_updated_at,
            stato_compressori_updated_at=outcome.stato_compressori_updated_at,
            current_sala_upserted=outcome.current_sala_upserted,
            current_compressori_upserted=outcome.current_compressori_upserted,
        )
        log_pipeline(
            source.display_name,
            sala_code,
            event="lag",
            csv_max_timestamp=csv_max_timestamp,
            db_last_timestamp_before_import=since_ts,
            db_updated_at=db_updated_at,
            lag_csv_to_db_seconds=lag_csv_to_db_seconds,
            lag_csv_to_now_seconds=lag_csv_to_now_seconds,
        )
        self._remember_source_state(source, checked_at=seen_at)

        if outcome.compressori_missing:
            LOGGER.warning("Compressori mancanti per %s: %s", source.display_name, outcome.compressori_missing)
        if outcome.warnings:
            for warning in outcome.warnings:
                LOGGER.warning(warning)
        if outcome.sala_rows_inserted == 0 and outcome.compressore_rows_inserted == 0:
            LOGGER.warning("No rows inserted for %s after DB write attempt", source.display_name)
            return

        if outcome.aggregate_range_start and outcome.aggregate_range_end:
            try:
                sale_results = refresh_pyramid_range_safely(
                    self.db.engine,
                    "sale",
                    outcome.aggregate_range_start,
                    outcome.aggregate_range_end,
                    entity_ids=[sala_id],
                    truncate_target_range=True,
                    allow_upsert_fallback=True,
                    logger=LOGGER,
                )
                log_pipeline(
                    source.display_name,
                    sala_code,
                    event="aggregate_refresh_sale",
                    aggregate_levels=[result.granularity for result in sale_results],
                    aggregate_range_start=outcome.aggregate_range_start,
                    aggregate_range_end=outcome.aggregate_range_end,
                )
            except Exception:
                LOGGER.exception(
                    "Aggregate refresh failed for sale=%s after ingest. Raw/current-state data were already committed.",
                    sala_code,
                )

            if outcome.compressore_rows_inserted > 0:
                try:
                    compressori_results = refresh_pyramid_range_safely(
                        self.db.engine,
                        "compressori",
                        outcome.aggregate_range_start,
                        outcome.aggregate_range_end,
                        entity_ids=[sala_id],
                        truncate_target_range=True,
                        allow_upsert_fallback=True,
                        logger=LOGGER,
                    )
                    log_pipeline(
                        source.display_name,
                        sala_code,
                        event="aggregate_refresh_compressori",
                        aggregate_levels=[result.granularity for result in compressori_results],
                        aggregate_range_start=outcome.aggregate_range_start,
                        aggregate_range_end=outcome.aggregate_range_end,
                    )
                except Exception:
                    LOGGER.exception(
                        "Aggregate refresh failed for compressori sala=%s after ingest. Raw/current-state data were already committed.",
                        sala_code,
                    )

        LOGGER.info(
            "Inserted new data from %s sala=%s rows_sale=%s rows_compressori=%s",
            source.display_name,
            sala_code,
            outcome.sala_rows_inserted,
            outcome.compressore_rows_inserted,
        )
        self._write_status(
            phase=mode,
            last_error=None,
            last_insert_at=utcnow_naive().isoformat(),
            last_insert_file=source.display_name,
            last_insert_sala=sala_code,
            last_insert_rows_sale=outcome.sala_rows_inserted,
            last_insert_rows_compressori=outcome.compressore_rows_inserted,
        )

    def _poll_target_days(self) -> list[date]:
        today = datetime.now(self.source_tz).date()
        return [today - timedelta(days=offset) for offset in range(POLL_LOOKBACK_DAYS)]

    def _should_fetch_source(self, source: RemoteCsvFile, checked_at: datetime) -> tuple[bool, str]:
        existing = self.runtime_state.get(source.source_url.lower())
        if existing is None:
            return True, "first_seen"
        if source.last_modified and existing.last_http_last_modified and source.last_modified > existing.last_http_last_modified:
            return True, "http_last_modified"
        if source.bytes_read and existing.last_bytes_read and source.bytes_read != existing.last_bytes_read:
            return True, "bytes_changed"
        if existing.last_checked_at is None:
            return True, "missing_last_checked"
        if seconds_between(checked_at, existing.last_checked_at) is not None and (
            checked_at - existing.last_checked_at
        ).total_seconds() >= SOURCE_RECHECK_SECONDS:
            return True, "periodic_recheck"
        return False, "hint_unchanged"

    def _remember_source_state(self, source: RemoteCsvFile, checked_at: datetime | None = None) -> None:
        self.runtime_state[source.source_url.lower()] = FileRuntimeState(
            last_http_last_modified=source.last_modified,
            last_bytes_read=source.bytes_read,
            last_checked_at=checked_at or utcnow_naive(),
        )

    def _validate_target_database(self) -> None:
        db_name = inspect(self.engine).default_schema_name
        expected_name = expected_database_name(self.config.db_url)
        if db_name and expected_name and db_name.lower() != expected_name.lower():
            LOGGER.warning("Connected schema is '%s', expected '%s'", db_name, expected_name)

    def _log_schema(self) -> None:
        return


def expected_database_name(database_url: str) -> str | None:
    parsed = urlparse(database_url)
    path = (parsed.path or "").strip("/")
    return path or None


def utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def seconds_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return round((later - earlier).total_seconds(), 3)


def log_pipeline(file_name: str, sala_code: str | None = None, **fields: Any) -> None:
    if not PIPELINE_DEBUG_ENABLED:
        return
    payload: list[str] = [f"file={file_name}"]
    if sala_code:
        payload.append(f"sala={sala_code}")
    for key, value in fields.items():
        if isinstance(value, datetime):
            rendered = value.isoformat()
        else:
            rendered = value
        payload.append(f"{key}={rendered}")
    LOGGER.info("[PIPELINE] %s", " ".join(str(item) for item in payload))


def build_date_range(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        raise ValueError(f"Invalid date range: {start_date.isoformat()} > {end_date.isoformat()}")
    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def resolve_database_url(explicit_url: str | None) -> str:
    if explicit_url:
        return explicit_url

    for env_name in ("GFR_ENERGYSAVING_DATABASE_URL", "SCADA_ENERGY_DATABASE_URL", "MYSQL_DATABASE_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            return value

    env_database_url = os.getenv("DATABASE_URL", "").strip()
    if env_database_url.lower().startswith("mysql"):
        return env_database_url

    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "3306").strip() or "3306"
    user = os.getenv("DB_USER", "").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    driver = os.getenv("DB_DRIVER", "mysql+pymysql").strip() or "mysql+pymysql"

    if all([host, user, password, name]):
        return (
            f"{driver}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{quote_plus(name)}"
        )

    configured = {
        "DB_HOST": bool(host),
        "DB_PORT": bool(port),
        "DB_USER": bool(user),
        "DB_PASSWORD": bool(password),
        "DB_NAME": bool(name),
    }
    missing = [key for key, present in configured.items() if not present]

    raise RuntimeError(
        "Energy-saving database configuration missing. Set GFR_ENERGYSAVING_DATABASE_URL, "
        "SCADA_ENERGY_DATABASE_URL, MYSQL_DATABASE_URL, a MySQL DATABASE_URL, or all DB_* env vars "
        f"(missing: {', '.join(missing) if missing else 'unknown'})."
    )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def parse_args() -> AppConfig:
    parser = argparse.ArgumentParser(description=f"Ingest SCADA CSV files into MySQL {TARGET_DATABASE_NAME}.")
    parser.add_argument(
        "--base-url",
        default=HARDCODED_BASE_CSV_URL.strip() or os.getenv("BASE_CSV_URL", "").strip() or DEFAULT_BASE_CSV_URL,
        help="Base HTTP URL containing the dated SCADA CSV folders.",
    )
    parser.add_argument(
        "--db-url",
        default=(
            os.getenv("GFR_ENERGYSAVING_DATABASE_URL", "").strip()
            or os.getenv("SCADA_ENERGY_DATABASE_URL", "").strip()
            or os.getenv("MYSQL_DATABASE_URL", "").strip()
            or os.getenv("DATABASE_URL", "").strip()
        ),
        help="Explicit SQLAlchemy MySQL URL. Overrides env vars.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=max(1, int(os.getenv("SCADA_POLL_SECONDS", str(DEFAULT_POLL_SECONDS)))),
        help="Polling interval in seconds after the initial import.",
    )
    parser.add_argument("--once", action="store_true", help="Import a single day and exit. Default day is yesterday.")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        default=os.getenv("SCADA_REIMPORT_EXISTING", "").strip().lower() in {"1", "true", "yes", "on"},
        help="Delete and reimport rows for the selected time window so existing timestamps are refreshed too.",
    )
    parser.add_argument(
        "--target-date",
        default=os.getenv("SCADA_TARGET_DATE", "").strip(),
        help="Single target date in YYYY-MM-DD. If omitted with --once, yesterday is used.",
    )
    parser.add_argument(
        "--from-date",
        default=os.getenv("SCADA_FROM_DATE", "").strip(),
        help="Backfill start date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--to-date",
        default=os.getenv("SCADA_TO_DATE", "").strip(),
        help="Backfill end date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("SCADA_LOG_LEVEL", DEFAULT_LOG_LEVEL),
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--status-file",
        default=os.getenv("SCADA_STATUS_FILE", DEFAULT_STATUS_FILE),
        help="Path of the heartbeat/status JSON file written by the ingestor.",
    )
    args = parser.parse_args()

    target_date = date.fromisoformat(args.target_date) if args.target_date else None
    from_date = date.fromisoformat(args.from_date) if args.from_date else None
    to_date = date.fromisoformat(args.to_date) if args.to_date else None
    if target_date and (from_date or to_date):
        raise SystemExit("Use either --target-date or --from-date/--to-date, not both.")
    return AppConfig(
        base_csv_url=str(args.base_url).rstrip("/"),
        db_url=resolve_database_url(args.db_url or None),
        poll_seconds=max(1, int(args.poll_seconds)),
        run_once=bool(args.once),
        replace_existing=bool(args.replace_existing),
        status_file=str(args.status_file),
        log_level=str(args.log_level).upper(),
        target_date=target_date,
        from_date=from_date,
        to_date=to_date,
    )


def main() -> None:
    config = parse_args()
    configure_logging(config.log_level)
    ingestor = CsvToDbIngestor(config)
    ingestor.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except (RuntimeError, SQLAlchemyError, ValueError) as exc:
        LOGGER.error(str(exc))
        raise
