import os
import socket
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_DATABASE_URL = (
    os.getenv("LOCAL_LEGACY_DATABASE_URL", "").strip()
    or f"sqlite:///{(Path(__file__).resolve().parents[2] / 'local_legacy.db').as_posix()}"
)
LOCAL_APP_ENVS = {"development", "dev", "local"}


def _is_local_app_env() -> bool:
    return os.getenv("APP_ENV", "development").strip().lower() in LOCAL_APP_ENVS


def _host_reachable(hostname: str, port: int | None) -> bool:
    if not hostname:
        return False

    try:
        with socket.create_connection((hostname, port or 0), timeout=0.35):
            return True
    except OSError:
        return False


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        return DEFAULT_DATABASE_URL

    try:
        parsed = urlparse(database_url)
        hostname = (parsed.hostname or "").strip().lower()
        scheme = (parsed.scheme or "").strip().lower()

        if hostname == "db":
            try:
                socket.getaddrinfo("db", None)
                return database_url
            except socket.gaierror:
                return DEFAULT_DATABASE_URL

        if _is_local_app_env() and scheme.startswith("postgres") and hostname in {"localhost", "127.0.0.1"}:
            if not _host_reachable(hostname, parsed.port or 5432):
                return DEFAULT_DATABASE_URL

        return database_url
    except Exception:
        return database_url
