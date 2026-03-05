import os
from dataclasses import dataclass


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(',') if v.strip()]


@dataclass(frozen=True)
class Settings:
    app_env: str
    cors_allow_origins: list[str]
    base_csv_url: str
    ingest_poll_seconds: int
    ingest_autostart: bool
    ingest_plants: list[str]
    ingest_source_timezone: str
    jwt_secret: str
    access_token_expire_seconds: int
    refresh_token_expire_days: int
    refresh_cookie_name: str
    refresh_cookie_secure: bool
    refresh_cookie_samesite: str


def load_settings() -> Settings:
    app_env = os.getenv('APP_ENV', 'development')

    jwt_secret = os.getenv('JWT_SECRET', '').strip()
    if not jwt_secret:
        raise RuntimeError('JWT_SECRET is required')

    origins_raw = os.getenv('CORS_ALLOW_ORIGINS', 'http://localhost:5173')
    cors_origins = _split_csv(origins_raw)
    ingest_plants_raw = os.getenv('INGEST_PLANTS', '')

    return Settings(
        app_env=app_env,
        cors_allow_origins=cors_origins,
        base_csv_url=os.getenv('BASE_CSV_URL', 'http://94.138.172.234:46812/shared').rstrip('/'),
        ingest_poll_seconds=max(1, int(os.getenv('INGEST_POLL_SECONDS', '5'))),
        ingest_autostart=os.getenv('INGEST_AUTOSTART', 'true').lower() == 'true',
        ingest_plants=_split_csv(ingest_plants_raw),
        ingest_source_timezone=os.getenv('INGEST_SOURCE_TIMEZONE', 'Europe/Rome').strip() or 'Europe/Rome',
        jwt_secret=jwt_secret,
        access_token_expire_seconds=int(os.getenv('ACCESS_TOKEN_EXPIRE_SECONDS', '300')),
        refresh_token_expire_days=int(os.getenv('REFRESH_TOKEN_EXPIRE_DAYS', '7')),
        refresh_cookie_name=os.getenv('REFRESH_COOKIE_NAME', 'refresh_token'),
        refresh_cookie_secure=os.getenv('REFRESH_COOKIE_SECURE', 'false').lower() == 'true',
        refresh_cookie_samesite=os.getenv('REFRESH_COOKIE_SAMESITE', 'lax'),
    )


settings = load_settings()
