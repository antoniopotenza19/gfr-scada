import api from './client'
import { TimeseriesPoint, AlarmEvent, PlantSummary } from '../types/api'

export async function fetchPlants(): Promise<string[]> {
  const resp = await api.get('/api/plants')
  return resp.data
}

export async function fetchPlantSummary(plant: string): Promise<PlantSummary> {
  const resp = await api.get(`/api/plants/${plant}/summary`)
  return resp.data
}

export async function fetchTimeseries(
  plant: string,
  signal: string,
  from?: string,
  to?: string,
  bucket?: string
): Promise<TimeseriesPoint[]> {
  const params: any = { signal }
  if (from) params.from = from
  if (to) params.to = to
  if (bucket) params.bucket = bucket
  const resp = await api.get(`/api/plants/${plant}/timeseries`, { params })
  return resp.data
}

export async function fetchAlarms(
  plant: string,
  from?: string,
  to?: string,
  limit: number = 100
): Promise<AlarmEvent[]> {
  const params: any = { limit }
  if (from) params.from = from
  if (to) params.to = to
  const resp = await api.get(`/api/plants/${plant}/alarms`, { params })
  return resp.data
}
