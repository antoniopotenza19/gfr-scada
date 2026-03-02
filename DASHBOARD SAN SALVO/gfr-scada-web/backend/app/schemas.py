from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = 'bearer'


class LoginRequest(BaseModel):
    username: str
    password: str


class CommandRequestIn(BaseModel):
    command: str
    target: Optional[str]
    params: Optional[str]


class TimeseriesPoint(BaseModel):
    ts: str
    value: float


class AlarmEvent(BaseModel):
    code: str
    severity: str
    message: str
    ts: str


class PlantSummary(BaseModel):
    plant: str
    last_update: str
    signals: Dict[str, Dict[str, Any]]
    compressors: List[Dict[str, Any]]
    dryers: List[Dict[str, Any]]
    active_alarms: List[AlarmEvent]


class PlantList(BaseModel):
    plants: List[str]
