from datetime import datetime, timedelta

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from . import models
from .config import settings
from .db import get_db

pwd_context = CryptContext(schemes=['bcrypt_sha256'], deprecated='auto')
security = HTTPBearer()

JWT_SECRET = settings.jwt_secret
ACCESS_EXPIRE = settings.access_token_expire_seconds
REFRESH_EXPIRE_DAYS = settings.refresh_token_expire_days

SITE_IDS = {'san-salvo', 'marghera'}


def normalize_role(role: str | None) -> str:
    value = (role or '').strip().lower()
    if value in {'san_salvo_viewer', 'san_salso_viewer', 'san-salvo-viewer'}:
        return 'san_salvo_viewer'
    if value == 'marghera_viewer':
        return 'marghera_viewer'
    if value in {'gfr', 'dev'}:
        return value

    # Legacy compatibility
    if value == 'viewer':
        return 'san_salvo_viewer'
    if value == 'operator':
        return 'gfr'
    if value == 'admin':
        return 'dev'

    return 'unknown'


def allowed_site_ids_for_role(role: str | None) -> set[str]:
    normalized = normalize_role(role)
    if normalized == 'san_salvo_viewer':
        return {'san-salvo'}
    if normalized == 'marghera_viewer':
        return {'marghera'}
    if normalized in {'gfr', 'dev'}:
        return set(SITE_IDS)
    return set()


def can_view_site(role: str | None, site_id: str) -> bool:
    normalized_site = (site_id or '').strip().lower()
    if normalized_site not in SITE_IDS:
        return False
    return normalized_site in allowed_site_ids_for_role(role)


def can_view_dev_features(role: str | None) -> bool:
    return normalize_role(role) == 'dev'


def can_remote_control(role: str | None) -> bool:
    return normalize_role(role) in {'gfr', 'dev'}


def ensure_site_access(role: str | None, site_id: str):
    if not can_view_site(role, site_id):
        raise HTTPException(status_code=403, detail='Not authorized for site')


def ensure_remote_control(role: str | None, site_id: str):
    ensure_site_access(role, site_id)
    if not can_remote_control(role):
        raise HTTPException(status_code=403, detail='Role not authorized for remote control')


def _role_rank(role: str | None) -> int:
    normalized = normalize_role(role)
    if normalized == 'dev':
        return 100
    if normalized == 'gfr':
        return 50
    if normalized in {'san_salvo_viewer', 'marghera_viewer'}:
        return 10
    return 0


def _required_rank(required: str) -> int:
    value = required.strip().lower()
    if value in {'admin', 'dev'}:
        return 100
    if value in {'operator', 'gfr'}:
        return 50
    if value in {'viewer', 'san_salvo_viewer', 'san_salso_viewer', 'marghera_viewer'}:
        return 10
    return 0


def hash_password(password: str) -> str:
    if password is None:
        password = ''
    try:
        return pwd_context.hash(password)
    except ValueError:
        pw = password[:72]
        return pwd_context.hash(pw)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        try:
            return pwd_context.verify(plain[:72], hashed)
        except Exception:
            return False


def create_access_token(user_id: int, role: str):
    payload = {
        'sub': str(user_id),
        'role': role,
        'type': 'access',
        'exp': datetime.utcnow() + timedelta(seconds=ACCESS_EXPIRE),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def create_refresh_token(user_id: int):
    payload = {
        'sub': str(user_id),
        'type': 'refresh',
        'exp': datetime.utcnow() + timedelta(days=REFRESH_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def decode_token(token: str, expected_type: str | None = None):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    except JWTError:
        raise HTTPException(status_code=401, detail='Invalid token')

    if expected_type and payload.get('type') != expected_type:
        raise HTTPException(status_code=401, detail='Invalid token type')

    return payload


def get_current_user(token: str = Depends(security), db: Session = Depends(get_db)):
    data = decode_token(token.credentials, expected_type='access')
    user = db.query(models.User).filter(models.User.id == int(data['sub'])).first()
    if not user:
        raise HTTPException(status_code=401, detail='User not found')
    return user


def require_role(role: str):
    def checker(user: models.User = Depends(get_current_user)):
        if _role_rank(user.role) < _required_rank(role):
            raise HTTPException(status_code=403, detail='Insufficient role')
        return user

    return checker
