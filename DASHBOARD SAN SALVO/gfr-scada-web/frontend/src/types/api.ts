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
}

export interface PlantSummary {
  plant: string;
  last_update: string;
  signals: Record<string, { value: number; unit: string; ts: string }>;
  compressors: Array<{ id: string; running: boolean; local?: boolean; fault: boolean }>;
  dryers: Array<{ id: string; running: boolean; fault: boolean }>;
  active_alarms: AlarmEvent[];
}
