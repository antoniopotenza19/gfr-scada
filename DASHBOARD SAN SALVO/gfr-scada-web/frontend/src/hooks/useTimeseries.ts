import { useQuery } from '@tanstack/react-query'
import { fetchTimeseries } from '../api/plants'
import { TimeseriesPoint } from '../types/api'

interface Params {
  plant: string
  signal: string
  from?: string
  to?: string
  bucket?: string
}

export function useTimeseries({ plant, signal, from, to, bucket }: Params) {
  return useQuery<TimeseriesPoint[]>(
    ['timeseries', plant, signal, from, to, bucket],
    () => fetchTimeseries(plant, signal, from, to, bucket),
    {
      enabled: Boolean(plant && signal),
    }
  )
}
