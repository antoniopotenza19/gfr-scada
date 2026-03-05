import { useQuery } from '@tanstack/react-query'
import { fetchAlarms } from '../api/plants'
import { AlarmEvent } from '../types/api'

export function useAlarms(plant: string, from?: string, to?: string, room?: string, limit: number = 100) {
  return useQuery<AlarmEvent[]>({
    queryKey: ['alarms', plant, room ?? null, from, to, limit],
    queryFn: () => fetchAlarms(plant, from, to, limit, room),
    enabled: Boolean(plant),
    staleTime: 5_000,
    cacheTime: 120_000,
    retry: 1,
    refetchInterval: 15_000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
  })
}
