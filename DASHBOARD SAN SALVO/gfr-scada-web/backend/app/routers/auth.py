from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from ..db import get_db
from .. import models
from .. import auth as _auth
from ..schemas import LoginRequest, TokenResponse

router = APIRouter(prefix="/api/auth")


@router.post('/login', response_model=TokenResponse)
def login(payload: LoginRequest, response: Response, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == payload.username).first()
    if not user or not _auth.verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail='Invalid credentials')

    access = _auth.create_access_token(user.id, user.role)
    refresh = _auth.create_refresh_token(user.id)

    # set refresh token in httpOnly cookie
    response.set_cookie('refresh_token', refresh, httponly=True, samesite='lax')
    return {'access_token': access}


@router.post('/refresh', response_model=TokenResponse)
def refresh(response: Response, db: Session = Depends(get_db), refresh_token: str = None):
    # read from cookie if not provided
    # NOTE: FastAPI does not provide cookies directly here without Request; keep simple for demo
    raise HTTPException(status_code=501, detail='Refresh not implemented in this demo; use login')
