import os
import socket
from urllib.parse import urlparse

DEFAULT_DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/gfr_db"


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        return DEFAULT_DATABASE_URL

    try:
        parsed = urlparse(database_url)
        if parsed.hostname != "db":
            return database_url
        socket.getaddrinfo("db", None)
        return database_url
    except socket.gaierror:
        return DEFAULT_DATABASE_URL
    except Exception:
        return database_url
