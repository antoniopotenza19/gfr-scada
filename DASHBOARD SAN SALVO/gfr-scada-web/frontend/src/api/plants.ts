import api from './client'
import { TimeseriesPoint, AlarmEvent, PlantSummary } from '../types/api'

export async function fetchPlants(): Promise<string[]> {
  const resp = await api.get('/api/plants')
  return resp.data
}

export async function fetchPlantSummary(plant: string, room?: string): Promise<PlantSummary> {
  const params: any = {}
  if (room) params.room = room
  const startedAt = typeof performance !== 'undefined' ? performance.now() : Date.now()
  if (import.meta.env.DEV) {
    console.debug('[SUMMARY][frontend]', {
      phase: 'request_start',
      plant,
      room: room ?? null,
      requestedAt: new Date().toISOString(),
    })
  }
  const resp = await api.get(`/api/plants/${plant}/summary`, { params })
  const finishedAt = typeof performance !== 'undefined' ? performance.now() : Date.now()
  if (import.meta.env.DEV) {
    console.debug('[SUMMARY][frontend]', {
      phase: 'request_end',
      plant,
      room: room ?? null,
      durationMs: Number((finishedAt - startedAt).toFixed(2)),
      receivedAt: new Date().toISOString(),
      lastUpdate: resp.data?.last_update ?? null,
      responseSize: Object.keys(resp.data?.signals || {}).length,
    })
  }
  return resp.data
}

export async function fetchTimeseries(
  plant: string,
  signal: string,
  from?: string,
  to?: string,
  bucket?: string,
  minutes?: number,
  maxPoints?: number,
  agg?: 'avg' | 'sum' | 'min' | 'max',
  room?: string
): Promise<TimeseriesPoint[]> {
  const params: any = { signal }
  if (from) params.from = from
  if (to) params.to = to
  if (bucket) params.bucket = bucket
  if (typeof minutes === 'number') params.minutes = minutes
  if (typeof maxPoints === 'number') params.max_points = maxPoints
  if (agg) params.agg = agg
  if (room) params.room = room
  const resp = await api.get(`/api/plants/${plant}/timeseries`, { params })
  const payload = resp.data
  if (Array.isArray(payload)) return payload as TimeseriesPoint[]
  if (payload && Array.isArray(payload.points)) return payload.points as TimeseriesPoint[]
  return []
}

export async function fetchMergedTimeseriesCandidates(
  plant: string,
  signals: string[],
  from?: string,
  to?: string,
  bucket?: string,
  minutes?: number,
  maxPoints?: number,
  agg?: 'avg' | 'sum' | 'min' | 'max',
  room?: string
): Promise<TimeseriesPoint[]> {
  const seen = new Set<string>()
  const candidates = signals.filter((signal) => {
    const key = signal.trim()
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })

  const merged = new Map<string, number>()
  for (const signal of candidates) {
    let points: TimeseriesPoint[] = []
    try {
      points = await fetchTimeseries(plant, signal, from, to, bucket, minutes, maxPoints, agg, room)
    } catch (error: any) {
      const status = Number(error?.response?.status || 0)
      if (status === 404 || status === 400) {
        if (import.meta.env.DEV) {
          console.debug('[TIMESERIES][skip]', {
            plant,
            room: room ?? null,
            signal,
            status,
          })
        }
        continue
      }
      throw error
    }
    for (const point of points) {
      merged.set(point.ts, (merged.get(point.ts) || 0) + (Number(point.value) || 0))
    }
  }

  return Array.from(merged.entries())
    .sort((a, b) => new Date(a[0]).getTime() - new Date(b[0]).getTime())
    .map(([ts, value]) => ({ ts, value }))
}

export async function fetchAlarms(
  plant: string,
  from?: string,
  to?: string,
  limit: number = 100,
  room?: string
): Promise<AlarmEvent[]> {
  const params: any = { limit }
  if (from) params.from = from
  if (to) params.to = to
  if (room) params.room = room
  const resp = await api.get(`/api/plants/${plant}/alarms`, { params })
  return resp.data
}
