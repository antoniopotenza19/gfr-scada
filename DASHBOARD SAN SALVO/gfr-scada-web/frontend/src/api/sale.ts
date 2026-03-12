import api from './client'
import type { ChartRangeKey } from '../constants/salaCharts'
import type { SaleChartsResponse, SaleCompressorActivityResponse } from '../types/api'

interface SaleQueryOptions {
  range?: ChartRangeKey
  from?: string
  to?: string
  maxPoints?: number
}

export async function fetchSaleCharts(
  saleId: string,
  options: SaleQueryOptions
): Promise<SaleChartsResponse> {
  const params: Record<string, string | number> = {}
  if (options.range) params.range = options.range
  if (options.from) params.from = options.from
  if (options.to) params.to = options.to
  if (typeof options.maxPoints === 'number') params.max_points = options.maxPoints
  const resp = await api.get(`/api/sale/${encodeURIComponent(saleId)}/timeseries`, { params })
  return resp.data as SaleChartsResponse
}

export async function fetchSaleCompressorActivity(
  saleId: string,
  options: SaleQueryOptions
): Promise<SaleCompressorActivityResponse> {
  const params: Record<string, string | number> = {}
  if (options.range) params.range = options.range
  if (options.from) params.from = options.from
  if (options.to) params.to = options.to
  const resp = await api.get(`/api/sale/${encodeURIComponent(saleId)}/compressors/activity`, { params })
  return resp.data as SaleCompressorActivityResponse
}
