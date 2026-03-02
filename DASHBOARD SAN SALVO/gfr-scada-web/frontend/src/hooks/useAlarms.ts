import { useQuery } from '@tanstack/react-query'
import { fetchAlarms } from '../api/plants'
import { AlarmEvent } from '../types/api'

export function useAlarms(plant: string, from?: string, to?: string) {
  return useQuery<AlarmEvent[]>(
    ['alarms', plant, from, to],
    () => fetchAlarms(plant, from, to),
    {
      enabled: Boolean(plant),
      refetchInterval: 10000,
    }
  )
}
