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


class PlantTimeseries(BaseModel):
    plant: str
    signal: str
    points: List[TimeseriesPoint]


class DashboardMonthlyOverview(BaseModel):
    plant: str
    source_table: str
    granularity: str
    from_ts: str
    to_ts: str
    range_has_data: bool
    volume_points: List[TimeseriesPoint]
    energy_points: List[TimeseriesPoint]


class AlarmEvent(BaseModel):
    id: Optional[str] = None
    code: str
    severity: str
    message: str
    ts: str
    room: Optional[str] = None
    plant: Optional[str] = None
    active: Optional[bool] = None
    ack_user: Optional[str] = None
    ack_time: Optional[str] = None


class AlarmCreateIn(BaseModel):
    room: Optional[str] = None
    signal: str
    severity: str
    message: str


class PlantSummary(BaseModel):
    plant: str
    last_update: Optional[str] = None
    signals: Dict[str, Dict[str, Any]]
    compressors: List[Dict[str, Any]]
    dryers: List[Dict[str, Any]]
    active_alarms: List[AlarmEvent]


class PlantList(BaseModel):
    plants: List[str]


class SaleChartPoint(BaseModel):
    timestamp: str
    pressione: Optional[float] = None
    pressione2: Optional[float] = None
    potenza_kw: Optional[float] = None
    cons_specifico: Optional[float] = None
    flusso_nm3h: Optional[float] = None
    dewpoint: Optional[float] = None
    temperatura: Optional[float] = None
    temperatura2: Optional[float] = None


class SaleChartsResponse(BaseModel):
    sale: str
    sale_name: Optional[str] = None
    plant: Optional[str] = None
    last_update: Optional[str] = None
    from_ts: str
    to_ts: str
    available_from_ts: Optional[str] = None
    available_to_ts: Optional[str] = None
    requested_range: Optional[str] = None
    granularity: str
    source_table: str
    range_has_data: bool
    points: List[SaleChartPoint]


class CompressorActivityItem(BaseModel):
    id_compressore: int
    code: str
    name: str
    current_state: str
    dominant_state: str
    minutes_on: float
    minutes_standby: float
    minutes_off: float
    utilization_pct: float
    standby_pct: float
    off_pct: float
    energy_kwh: Optional[float] = None
    avg_power_kw: Optional[float] = None


class SaleCompressorActivityResponse(BaseModel):
    sale: str
    sale_name: Optional[str] = None
    plant: Optional[str] = None
    from_ts: str
    to_ts: str
    available_from_ts: Optional[str] = None
    available_to_ts: Optional[str] = None
    requested_range: Optional[str] = None
    granularity: str
    source_table: str
    range_has_data: bool
    items: List[CompressorActivityItem]
