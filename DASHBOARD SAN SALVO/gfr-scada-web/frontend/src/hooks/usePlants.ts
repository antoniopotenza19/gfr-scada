import { useQuery } from '@tanstack/react-query'
import { fetchPlants } from '../api/plants'

export function usePlants() {
  return useQuery(['plants'], fetchPlants)
}
