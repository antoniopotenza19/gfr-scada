import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("CORS_ALLOW_ORIGINS", "http://localhost:5173")
os.environ.setdefault("INGEST_AUTOSTART", "false")

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.auth import hash_password
from app.db import get_db
from app.db.base import Base
from app.main import app
from app.models import AlarmEvent as AlarmEventModel
from app.models import Plant, User


@pytest.fixture()
def alarm_history_client():
    db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_file.close()

    engine = create_engine(
        f"sqlite:///{db_file.name}",
        connect_args={"check_same_thread": False},
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    db = TestingSessionLocal()
    db.add(User(username="admin", hashed_password=hash_password("admin123!"), role="admin"))
    db.add(Plant(name="MAIN_PLANT"))
    db.commit()
    db.close()

    with TestClient(app) as client:
        try:
            yield client, TestingSessionLocal
        finally:
            app.dependency_overrides.clear()

    Base.metadata.drop_all(bind=engine)
    engine.dispose()
    os.unlink(db_file.name)


def test_alarms_returns_historical_rows_sorted_desc(alarm_history_client):
    client, Session = alarm_history_client
    db = Session()
    now = datetime.now(UTC)
    db.add_all(
        [
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS2",
                signal="PT-060",
                severity="critical",
                message="Caduta pressione rete",
                active=True,
                ts=now - timedelta(minutes=45),
            ),
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS2",
                signal="AT-061",
                severity="high",
                message="Dew point fuori specifica",
                active=False,
                ack_user="operator",
                ack_time=now - timedelta(minutes=10),
                ts=now - timedelta(minutes=20),
            ),
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS1",
                signal="CMP-204",
                severity="high",
                message="Compressore fault",
                active=True,
                ts=now - timedelta(minutes=5),
            ),
        ]
    )
    db.commit()
    db.close()

    response = client.get("/api/plants/SS2/alarms")
    assert response.status_code == 200

    body = response.json()
    assert [item["code"] for item in body] == ["AT-061", "PT-060"]
    assert body[0]["room"] == "SS2"
    assert body[0]["active"] is False
    assert body[0]["ack_user"] == "operator"
    assert body[0]["ack_time"].endswith("Z")


def test_alarms_supports_from_to_and_limit_filters(alarm_history_client):
    client, Session = alarm_history_client
    db = Session()
    base = datetime(2026, 3, 14, 12, 0, tzinfo=UTC)
    db.add_all(
        [
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS2",
                signal="ALM-001",
                severity="info",
                message="Evento 1",
                active=True,
                ts=base,
            ),
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS2",
                signal="ALM-002",
                severity="high",
                message="Evento 2",
                active=True,
                ts=base + timedelta(hours=1),
            ),
            AlarmEventModel(
                plant="MAIN_PLANT",
                room="SS2",
                signal="ALM-003",
                severity="critical",
                message="Evento 3",
                active=True,
                ts=base + timedelta(hours=2),
            ),
        ]
    )
    db.commit()
    db.close()

    response = client.get(
        "/api/plants/MAIN_PLANT/alarms",
        params={
            "room": "SS2",
            "from": "2026-03-14T12:30:00Z",
            "to": "2026-03-14T14:00:00Z",
            "limit": 1,
        },
    )
    assert response.status_code == 200

    body = response.json()
    assert len(body) == 1
    assert body[0]["code"] == "ALM-003"
