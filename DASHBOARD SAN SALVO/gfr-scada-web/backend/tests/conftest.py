import os
import tempfile
from pathlib import Path
import sys
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('CORS_ALLOW_ORIGINS', 'http://localhost:5173')
os.environ.setdefault('INGEST_AUTOSTART', 'false')

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.main import app
from app.db import get_db
from app.db.base import Base
from app.models import User, Plant
from app.auth import hash_password


@pytest.fixture()
def client_and_db():
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_file.close()

    engine = create_engine(
        f'sqlite:///{db_file.name}',
        connect_args={'check_same_thread': False},
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
    client = TestClient(app)

    db = TestingSessionLocal()
    db.add(User(username='admin', hashed_password=hash_password('admin123!'), role='admin'))
    db.add(User(username='viewer', hashed_password=hash_password('viewer123!'), role='viewer'))
    db.add(User(username='operator', hashed_password=hash_password('operator123!'), role='operator'))
    db.add(Plant(name='DEMO_PLANT'))
    db.commit()
    db.close()

    try:
        yield client, TestingSessionLocal
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(bind=engine)
        os.unlink(db_file.name)
