import { useQuery } from '@tanstack/react-query'
import { fetchPlants } from '../api/plants'

export function usePlants() {
  return useQuery({
    queryKey: ['plants'],
    queryFn: fetchPlants,
    staleTime: 30_000,
    cacheTime: 300_000,
    refetchInterval: 30_000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  })
}
