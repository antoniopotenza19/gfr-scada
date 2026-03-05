import { useQuery } from '@tanstack/react-query'
import { fetchPlantSummary } from '../api/plants'

export function usePlantSummary(plant: string, enabled: boolean = true, room?: string) {
  const queryKey = ['plantSummary', plant, room ?? null] as const

  return useQuery({
    queryKey,
    queryFn: () => fetchPlantSummary(plant, room),
    enabled: Boolean(plant) && enabled,
    staleTime: 1_000,
    cacheTime: 120_000,
    keepPreviousData: false,
    retry: 1,
    refetchInterval: 10_000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
    onSuccess: (data) => {
      if (!import.meta.env.DEV) return
      console.debug('[RQ][plantSummary]', {
        plant,
        room: room ?? null,
        queryKey,
        lastFetchTime: new Date().toISOString(),
        responseSize: Object.keys(data?.signals || {}).length,
      })
    },
    onError: (error) => {
      if (!import.meta.env.DEV) return
      console.debug('[RQ][plantSummary][error]', {
        plant,
        room: room ?? null,
        queryKey,
        error: String(error),
      })
    },
  })
}
