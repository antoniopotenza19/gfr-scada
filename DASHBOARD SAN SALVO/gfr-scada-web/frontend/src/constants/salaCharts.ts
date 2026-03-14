export type ChartRangeKey = '5m' | '15m' | '30m' | '1h' | '1d' | '1w' | '1mo' | '3mo' | '6mo' | '1y'
export type SalaMetricKey = 'pressione' | 'pressione2' | 'potenza_kw' | 'cons_specifico' | 'flusso_nm3h' | 'dewpoint' | 'temperatura' | 'temperatura2'

export interface RangePreset {
  key: ChartRangeKey
  label: string
  icon?: string
  realtime: boolean
}

export interface ThresholdLine {
  label: string
  value: number
  color: string
}

export interface SalaMetricConfig {
  key: SalaMetricKey
  label: string
  unit: string
  color: string
  accent: string
  description: string
}

export const RANGE_PRESETS: RangePreset[] = [
  { key: '5m', label: '5 min', icon: '⏱', realtime: true },
  { key: '15m', label: '15 min', icon: '⏱', realtime: true },
  { key: '30m', label: '30 min', icon: '⏱', realtime: true },
  { key: '1h', label: '1 ora', icon: '⏱', realtime: true },
  { key: '1d', label: '1 giorno', icon: '📅', realtime: false },
  { key: '1w', label: '1 settimana', icon: '📅', realtime: false },
  { key: '1mo', label: '1 mese', icon: '📆', realtime: false },
  { key: '3mo', label: '3 mesi', icon: '📆', realtime: false },
  { key: '6mo', label: '6 mesi', icon: '📆', realtime: false },
  { key: '1y', label: '1 anno', icon: '📆', realtime: false },
]

export const DEFAULT_REALTIME_RANGE: ChartRangeKey = '15m'
export const INITIAL_SALA_CHART_RANGE: ChartRangeKey = '1mo'

export const SALA_METRICS: SalaMetricConfig[] = [
  {
    key: 'pressione',
    label: 'Pressione',
    unit: 'barg',
    color: '#0f766e',
    accent: 'from-[#e6fffb] to-[#f8fffe]',
    description: 'Linea principale aria compressa e sua stabilita nel range selezionato.',
  },
  {
    key: 'potenza_kw',
    label: 'Potenza Attiva',
    unit: 'kW',
    color: '#0284c7',
    accent: 'from-[#e8f4ff] to-[#f9fcff]',
    description: 'Assorbimento elettrico medio della sala compressori.',
  },
  {
    key: 'cons_specifico',
    label: 'Consumo Specifico',
    unit: 'kWh/Nm3',
    color: '#ea580c',
    accent: 'from-[#fff1e8] to-[#fffaf7]',
    description: 'Indicatore di efficienza energetica del sistema aria.',
  },
  {
    key: 'flusso_nm3h',
    label: 'Flusso',
    unit: 'Nm3/h',
    color: '#7c3aed',
    accent: 'from-[#f4ebff] to-[#fbf8ff]',
    description: 'Portata aria disponibile nel periodo richiesto.',
  },
  {
    key: 'dewpoint',
    label: 'Dew Point',
    unit: '°C',
    color: '#059669',
    accent: 'from-[#e8fff6] to-[#f7fffc]',
    description: 'Qualita aria e comportamento dell’essiccazione nel tempo.',
  },
  {
    key: 'temperatura',
    label: 'Temperatura',
    unit: '°C',
    color: '#dc2626',
    accent: 'from-[#fff1f2] to-[#fffafb]',
    description: 'Andamento termico della sala nel range selezionato.',
  },
]

const DEFAULT_THRESHOLD_MAP: Partial<Record<SalaMetricKey, ThresholdLine[]>> = {
  dewpoint: [
    { label: 'Max garantita', value: 3, color: '#dc2626' },
  ],
}

const THRESHOLD_OVERRIDES_BY_SALA: Partial<Record<string, Partial<Record<SalaMetricKey, ThresholdLine[]>>>> = {
  LAMINATO: {
    dewpoint: [
      { label: 'Dew point max', value: 10, color: '#dc2626' },
    ],
  },
  LAMINATI: {
    dewpoint: [
      { label: 'Dew point max', value: 10, color: '#dc2626' },
    ],
  },
}

const CS_REALIZZABILE_BY_SALA: Record<string, number> = {
  BRAVO: 0.103,
  CENTAC: 0.101,
  LAMINATO: 0.178,
  LAMINATI: 0.178,
  PRIMO_ALTA: 0.5,
  PRIMO_BASSA: 0.3,
  SS1: 0.102,
  SS2: 0.107,
  SS1_COMP: 0.102,
  SS2_COMP: 0.102,
  LAM_MP_7BAR: 0.184,
  LAM_ALTA: 0.184,
}

const CS_OPTIMAL_BY_SALA: Record<string, number> = {
  LAMINATO: 0.174,
  LAMINATI: 0.174,
}

export function getSalaMetricThresholds(saleCode: string, metric: SalaMetricKey): ThresholdLine[] {
  const normalizedSale = saleCode.trim().toUpperCase()
  if (metric === 'cons_specifico') {
    const lines: ThresholdLine[] = []
    const realizable = CS_REALIZZABILE_BY_SALA[normalizedSale]
    const optimal = CS_OPTIMAL_BY_SALA[normalizedSale]
    if (typeof realizable === 'number') {
      lines.push({ label: 'CS realizzabile', value: realizable, color: '#16a34a' })
    }
    if (typeof optimal === 'number') {
      lines.push({ label: 'CS ottimale', value: optimal, color: '#0ea5e9' })
    }
    return lines
  }
  const saleOverride = THRESHOLD_OVERRIDES_BY_SALA[normalizedSale]?.[metric]
  if (saleOverride) {
    return saleOverride
  }
  return DEFAULT_THRESHOLD_MAP[metric] || []
}
