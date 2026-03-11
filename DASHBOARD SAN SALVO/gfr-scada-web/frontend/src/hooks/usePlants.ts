import { useQuery } from '@tanstack/react-query'
import { fetchPlants } from '../api/plants'
import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'

export function usePlants() {
  return useQuery({
    queryKey: ['plants'],
    queryFn: fetchPlants,
    staleTime: LIVE_SUMMARY_REFRESH_MS,
    cacheTime: 300_000,
    refetchInterval: LIVE_SUMMARY_REFRESH_MS,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  })
}
