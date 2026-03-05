import json
from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .. import models
from ..auth import ensure_remote_control, get_current_user
from ..db import get_db

router = APIRouter(prefix='/api/sites', tags=['site-commands'])


class MachineCommandIn(BaseModel):
    command: Literal['START', 'STOP']


@router.post('/{site_id}/rooms/{room_id}/machines/{machine_id}/command')
def command_machine(
    site_id: str,
    room_id: str,
    machine_id: str,
    payload: MachineCommandIn,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    ensure_remote_control(user.role, site_id)

    cmd = models.CommandRequest(
        requested_by=user.id,
        command=payload.command,
        target=f'{site_id}:{room_id}:{machine_id}',
        params=json.dumps({'siteId': site_id, 'roomId': room_id, 'machineId': machine_id}),
        status='requested',
    )
    db.add(cmd)
    db.commit()
    db.refresh(cmd)

    return {'id': cmd.id, 'status': cmd.status, 'mode': 'queued'}
