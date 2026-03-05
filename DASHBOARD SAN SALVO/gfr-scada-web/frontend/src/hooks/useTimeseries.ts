import { useQuery } from '@tanstack/react-query'
import { fetchTimeseries } from '../api/plants'
import { TimeseriesPoint } from '../types/api'

interface Params {
  plant: string
  signal: string
  room?: string
  from?: string
  to?: string
  bucket?: string
  minutes?: number
  maxPoints?: number
  agg?: 'avg' | 'sum' | 'min' | 'max'
}

export function useTimeseries({ plant, signal, room, from, to, bucket, minutes, maxPoints, agg }: Params) {
  const queryKey = ['timeseries', plant, signal, room ?? null, from, to, bucket, minutes, maxPoints, agg] as const

  return useQuery<TimeseriesPoint[]>({
    queryKey,
    queryFn: () => fetchTimeseries(plant, signal, from, to, bucket, minutes, maxPoints, agg, room),
    enabled: Boolean(plant && signal),
    staleTime: 5_000,
    cacheTime: 120_000,
    keepPreviousData: true,
    retry: 1,
    refetchInterval: 15_000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
    onSuccess: (data) => {
      if (!import.meta.env.DEV) return
      console.debug('[RQ][timeseries]', {
        plant,
        room: room ?? null,
        signal,
        minutes: minutes ?? null,
        queryKey,
        lastFetchTime: new Date().toISOString(),
        responseSize: data.length,
      })
    },
    onError: (error) => {
      if (!import.meta.env.DEV) return
      console.debug('[RQ][timeseries][error]', {
        plant,
        room: room ?? null,
        signal,
        minutes: minutes ?? null,
        queryKey,
        error: String(error),
      })
    },
  })
}
