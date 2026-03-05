"""
Seed script: creates demo users and demo plant data.
Run: `python app/scripts/seed.py` inside container or virtualenv.
"""
import os
import socket
import sys
from urllib.parse import urlparse, urlunparse
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False

load_dotenv()

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.auth import hash_password
from app.models import Plant, Room, User
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def _resolve_database_url() -> str:
    raw = os.getenv('DATABASE_URL', '').strip()
    if not raw:
        raise SystemExit('DATABASE_URL not set')

    parsed = urlparse(raw)
    if parsed.hostname != 'db':
        return raw

    try:
        socket.getaddrinfo('db', None)
        return raw
    except socket.gaierror:
        user = parsed.username or 'postgres'
        password = parsed.password or 'postgres'
        port = parsed.port or 5432
        db_name = parsed.path or '/gfr_scada'
        netloc = f'{user}:{password}@localhost:{port}'
        return urlunparse((parsed.scheme, netloc, db_name, parsed.params, parsed.query, parsed.fragment))


DATABASE_URL = _resolve_database_url()

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def _is_local_dev() -> bool:
    return os.getenv('APP_ENV', 'development').strip().lower() in {'development', 'dev', 'local'}


def _bootstrap_default_admin(db) -> None:
    if not _is_local_dev():
        print('Skipping default admin bootstrap: APP_ENV is not local development.')
        return

    username = os.getenv('DEV_DEFAULT_ADMIN_USERNAME', 'admin').strip() or 'admin'
    password = os.getenv('DEV_DEFAULT_ADMIN_PASSWORD', 'admin123')

    if not db.query(User).filter(User.username == username).first():
        db.add(User(username=username, hashed_password=hash_password(password), role='admin'))


def _upsert_demo_users(db) -> None:
    """
    Demo credentials requested for current environment:
    - gfr / gfr                 -> role gfr
    - dev / dev                 -> role dev
    - sansalvo / sansalvo       -> role san_salvo_viewer
    - marghera / marghera       -> role marghera_viewer
    """
    demo_users = [
        ('gfr', 'gfr', 'gfr'),
        ('dev', 'dev', 'dev'),
        ('sansalvo', 'sansalvo', 'san_salvo_viewer'),
        ('marghera', 'marghera', 'marghera_viewer'),
    ]

    for username, password, role in demo_users:
        existing = db.query(User).filter(User.username == username).first()
        if existing:
            existing.hashed_password = hash_password(password)
            existing.role = role
        else:
            db.add(User(username=username, hashed_password=hash_password(password), role=role))


def seed():
    db = Session()
    try:
        _bootstrap_default_admin(db)
        _upsert_demo_users(db)

        plant = db.query(Plant).filter(Plant.name == 'DEMO_PLANT').first()
        if not plant:
            plant = Plant(name='DEMO_PLANT')
            db.add(plant)
            db.flush()
            db.add(Room(plant_id=plant.id, name='Main Room'))

        db.commit()
        print('Seed completed')
    finally:
        db.close()


if __name__ == '__main__':
    seed()
