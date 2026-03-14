import { useQuery } from '@tanstack/react-query'
import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'
import { fetchAlarms } from '../api/plants'
import { AlarmEvent } from '../types/api'

export function useAlarms(plant: string, from?: string, to?: string, room?: string, limit: number = 1000) {
  return useQuery<AlarmEvent[]>({
    queryKey: ['alarms', plant, room ?? null, from, to, limit],
    queryFn: () => fetchAlarms(plant, from, to, limit, room),
    enabled: Boolean(plant),
    staleTime: LIVE_SUMMARY_REFRESH_MS,
    cacheTime: 120_000,
    retry: 1,
    refetchInterval: LIVE_SUMMARY_REFRESH_MS,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
  })
}
