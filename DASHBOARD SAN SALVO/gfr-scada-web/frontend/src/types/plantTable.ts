export type PlantStatus = 'active' | 'idle' | 'nd' | 'dism'

export interface PlantSignalInfo {
  value: number
  unit: string
  ts: string
}

export interface PlantRow {
  sala: string
  status: PlantStatus
  lastUpdate: string | null
  realtimeNm3: number | null
  realtimeKwh: number | null
  flussoMedio: number | null
  potenzaMedia: number | null
  pressioneMedia: number | null
  dewPointMedia: number | null
  temperaturaMedia: number | null
  csPeriodo: number | null
  csContratto: number | null
  percentEnergiaConsumata: number | null
  detailSignals?: Record<string, PlantSignalInfo>
}
