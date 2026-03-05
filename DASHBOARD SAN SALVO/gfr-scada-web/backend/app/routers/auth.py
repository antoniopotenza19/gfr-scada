from fastapi import APIRouter, Depends, HTTPException, Response, Request
from sqlalchemy.orm import Session
from ..db import get_db
from .. import models
from .. import auth as _auth
from ..config import settings
from ..schemas import LoginRequest, TokenResponse

router = APIRouter(prefix='/api/auth')


@router.post('/login', response_model=TokenResponse)
def login(payload: LoginRequest, response: Response, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == payload.username).first()
    if not user or not _auth.verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail='Invalid credentials')

    access = _auth.create_access_token(user.id, user.role)
    refresh = _auth.create_refresh_token(user.id)

    response.set_cookie(
        key=settings.refresh_cookie_name,
        value=refresh,
        httponly=True,
        secure=settings.refresh_cookie_secure,
        samesite=settings.refresh_cookie_samesite,
        max_age=settings.refresh_token_expire_days * 24 * 3600,
        path='/api/auth',
    )
    return {'access_token': access}


@router.post('/refresh', response_model=TokenResponse)
def refresh(request: Request, response: Response, db: Session = Depends(get_db)):
    refresh_token = request.cookies.get(settings.refresh_cookie_name)
    if not refresh_token:
        raise HTTPException(status_code=401, detail='Missing refresh token')

    payload = _auth.decode_token(refresh_token, expected_type='refresh')
    user = db.query(models.User).filter(models.User.id == int(payload['sub'])).first()
    if not user:
        raise HTTPException(status_code=401, detail='User not found')

    access = _auth.create_access_token(user.id, user.role)
    rotated_refresh = _auth.create_refresh_token(user.id)
    response.set_cookie(
        key=settings.refresh_cookie_name,
        value=rotated_refresh,
        httponly=True,
        secure=settings.refresh_cookie_secure,
        samesite=settings.refresh_cookie_samesite,
        max_age=settings.refresh_token_expire_days * 24 * 3600,
        path='/api/auth',
    )
    return {'access_token': access}
