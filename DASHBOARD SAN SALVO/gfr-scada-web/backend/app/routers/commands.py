from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..db import get_db
from .. import models
from ..auth import can_remote_control, ensure_remote_control, get_current_user, require_role
from ..schemas import CommandRequestIn

router = APIRouter(prefix='/api/commands')


def _extract_site_from_target(target: str | None) -> str | None:
    if not target:
        return None
    parts = [part.strip() for part in target.split(':') if part.strip()]
    if len(parts) < 3:
        return None
    return parts[0].lower()


@router.post('/request')
def request_command(payload: CommandRequestIn, db: Session = Depends(get_db), user: models.User = Depends(get_current_user)):
    if not can_remote_control(user.role):
        raise HTTPException(status_code=403, detail='Role not authorized for remote control')
    site_id = _extract_site_from_target(payload.target)
    if site_id:
        ensure_remote_control(user.role, site_id)
    cmd = models.CommandRequest(requested_by=user.id, command=payload.command, target=payload.target or '', params=payload.params or '')
    db.add(cmd)
    db.commit()
    db.refresh(cmd)
    return {'id': cmd.id, 'status': cmd.status}


@router.get('/list')
def list_commands(db: Session = Depends(get_db), user: models.User = Depends(require_role('viewer'))):
    return db.query(models.CommandRequest).order_by(models.CommandRequest.created_at.desc()).limit(100).all()
