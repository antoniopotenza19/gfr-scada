from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..db import get_db
from .. import models
from ..auth import require_role, get_current_user
from ..schemas import CommandRequestIn

router = APIRouter(prefix='/api/commands')


@router.post('/request')
def request_command(payload: CommandRequestIn, db: Session = Depends(get_db), user: models.User = Depends(require_role('operator'))):
    cmd = models.CommandRequest(requested_by=user.id, command=payload.command, target=payload.target or '', params=payload.params or '')
    db.add(cmd)
    db.commit()
    db.refresh(cmd)
    return {'id': cmd.id, 'status': cmd.status}


@router.get('/list')
def list_commands(db: Session = Depends(get_db), user: models.User = Depends(require_role('viewer'))):
    return db.query(models.CommandRequest).order_by(models.CommandRequest.created_at.desc()).limit(100).all()
