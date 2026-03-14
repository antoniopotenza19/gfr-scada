export interface TimeseriesPoint {
  ts: string;
  value: number;
}

export interface AlarmEvent {
  id?: string;
  code: string;
  severity: string;
  msg?: string;
  message: string;
  ts: string;
  room?: string;
  plant?: string;
  active?: boolean;
  ack_user?: string | null;
  ack_time?: string | null;
}

export interface PlantSummary {
  plant: string;
  last_update: string;
  signals: Record<string, { value: number; unit: string; ts: string }>;
  compressors: Array<{ id: string; running: boolean; local?: boolean; fault: boolean }>;
  dryers: Array<{ id: string; running: boolean; fault: boolean }>;
  active_alarms: AlarmEvent[];
}

export interface DashboardMonthlyOverview {
  plant: string;
  source_table: string;
  granularity: string;
  from_ts: string;
  to_ts: string;
  range_has_data: boolean;
  volume_points: TimeseriesPoint[];
  energy_points: TimeseriesPoint[];
}

export interface SaleChartPoint {
  timestamp: string
  pressione: number | null
  pressione2?: number | null
  potenza_kw: number | null
  cons_specifico: number | null
  flusso_nm3h: number | null
  dewpoint: number | null
  temperatura: number | null
  temperatura2?: number | null
}

export interface SaleChartsResponse {
  sale: string
  sale_name: string | null
  plant: string | null
  last_update: string | null
  from_ts: string
  to_ts: string
  available_from_ts: string | null
  available_to_ts: string | null
  requested_range: string | null
  granularity: string
  source_table: string
  range_has_data: boolean
  points: SaleChartPoint[]
}

export interface CompressorActivityItem {
  id_compressore: number
  code: string
  name: string
  current_state: string
  dominant_state: string
  minutes_on: number
  minutes_standby: number
  minutes_off: number
  utilization_pct: number
  standby_pct: number
  off_pct: number
  energy_kwh: number | null
  avg_power_kw: number | null
}

export interface SaleCompressorActivityResponse {
  sale: string
  sale_name: string | null
  plant: string | null
  from_ts: string
  to_ts: string
  available_from_ts: string | null
  available_to_ts: string | null
  requested_range: string | null
  granularity: string
  source_table: string
  range_has_data: boolean
  items: CompressorActivityItem[]
}
