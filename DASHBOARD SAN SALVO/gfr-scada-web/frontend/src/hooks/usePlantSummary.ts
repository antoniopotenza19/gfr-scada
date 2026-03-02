import { useQuery } from '@tanstack/react-query'
import { fetchPlantSummary } from '../api/plants'

export function usePlantSummary(plant: string, enabled: boolean = true) {
  return useQuery(['plantSummary', plant], () => fetchPlantSummary(plant), {
    enabled: Boolean(plant) && enabled,
    refetchInterval: 5000
  })
}
