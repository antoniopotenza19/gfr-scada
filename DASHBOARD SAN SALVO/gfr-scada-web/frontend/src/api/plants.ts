import api from './client'
import { TimeseriesPoint, AlarmEvent, PlantSummary } from '../types/api'

export async function fetchPlants(): Promise<string[]> {
  const resp = await api.get('/api/plants')
  return resp.data
}

export async function fetchPlantSummary(plant: string, room?: string): Promise<PlantSummary> {
  const params: any = {}
  if (room) params.room = room
  const resp = await api.get(`/api/plants/${plant}/summary`, { params })
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
