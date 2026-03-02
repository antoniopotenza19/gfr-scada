import os
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import jwt
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from .db import get_db
from . import models

"""Use bcrypt_sha256 to avoid bcrypt 72-byte limit (passlib handles sha256 pre-hash).
If bcrypt backend is missing or an unexpected ValueError occurs, fallback to truncation.
"""
pwd_context = CryptContext(schemes=["bcrypt_sha256"], deprecated="auto")
security = HTTPBearer()

JWT_SECRET = os.getenv('JWT_SECRET', 'CHANGE_ME')
ACCESS_EXPIRE = int(os.getenv('ACCESS_TOKEN_EXPIRE_SECONDS', '300'))
REFRESH_EXPIRE_DAYS = int(os.getenv('REFRESH_TOKEN_EXPIRE_DAYS', '7'))


def hash_password(password: str) -> str:
    # Ensure input is str
    if password is None:
        password = ''
    try:
        return pwd_context.hash(password)
    except ValueError:
        # bcrypt backend may raise for long passwords; truncate as last resort
        pw = password[:72]
        return pwd_context.hash(pw)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        # in case of backend mismatch, try truncated verification
        try:
            return pwd_context.verify(plain[:72], hashed)
        except Exception:
            return False


def create_access_token(user_id: int, role: str):
    payload = {
        'sub': user_id,
        'role': role,
        'exp': datetime.utcnow() + timedelta(seconds=ACCESS_EXPIRE)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def create_refresh_token(user_id: int):
    payload = {
        'sub': user_id,
        'exp': datetime.utcnow() + timedelta(days=REFRESH_EXPIRE_DAYS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def decode_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail='Invalid token')


def get_current_user(token: str = Depends(security), db: Session = Depends(get_db)):
    data = decode_token(token.credentials)
    user = db.query(models.User).filter(models.User.id == int(data['sub'])).first()
    if not user:
        raise HTTPException(status_code=401, detail='User not found')
    return user


def require_role(role: str):
    def checker(user: models.User = Depends(get_current_user)):
        roles = {'viewer': 10, 'operator': 50, 'admin': 100}
        if roles.get(user.role, 0) < roles.get(role, 0):
            raise HTTPException(status_code=403, detail='Insufficient role')
        return user
    return checker
