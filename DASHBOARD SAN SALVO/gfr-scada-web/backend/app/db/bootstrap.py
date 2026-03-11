import logging
import os

from ..auth import hash_password
from ..models import Plant, Room, User

from . import Base, SessionLocal, engine

logger = logging.getLogger(__name__)


def run_legacy_bootstrap() -> None:
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        default_users = [
            (
                os.getenv("DEV_DEFAULT_ADMIN_USERNAME", "admin").strip() or "admin",
                os.getenv("DEV_DEFAULT_ADMIN_PASSWORD", "admin123"),
                "dev",
            ),
            ("gfr", "gfr", "gfr"),
            ("dev", "dev", "dev"),
            ("sansalvo", "sansalvo", "san_salvo_viewer"),
            ("marghera", "marghera", "marghera_viewer"),
        ]

        for username, password, role in default_users:
            user = session.query(User).filter(User.username == username).first()
            if user:
                continue
            session.add(User(username=username, hashed_password=hash_password(password), role=role))

        plant = session.query(Plant).filter(Plant.name == "DEMO_PLANT").first()
        if not plant:
            plant = Plant(name="DEMO_PLANT")
            session.add(plant)
            session.flush()
            session.add(Room(plant_id=plant.id, name="Main Room"))

        session.commit()
        logger.info("Local legacy DB bootstrap ready")
    except Exception:
        session.rollback()
        logger.exception("Failed local legacy DB bootstrap")
        raise
    finally:
        session.close()
