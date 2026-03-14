import { useRef } from 'react'

import type { CompressorActivityItem } from '../../types/api'
import { exportElementAsPng } from '../../utils/exportImage'
import StatusBadge, { type CompressorStatus } from '../compressors/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card'
import Skeleton from '../ui/Skeleton'

interface CompressorActivityPanelProps {
  items: CompressorActivityItem[]
  loading?: boolean
  exportFileName?: string
  onExportData?: () => void
  onExportImage?: () => void
}

function DownloadIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M12 4v10m0 0 4-4m-4 4-4-4M5 18h14" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function ImageIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M4 6.5A2.5 2.5 0 0 1 6.5 4h11A2.5 2.5 0 0 1 20 6.5v11a2.5 2.5 0 0 1-2.5 2.5h-11A2.5 2.5 0 0 1 4 17.5v-11Z" fill="none" stroke="currentColor" strokeWidth="1.8" />
      <path d="m7 16 3.2-3.2a1 1 0 0 1 1.4 0L14 15l1.6-1.6a1 1 0 0 1 1.4 0L19 15.5M9 9.5h.01" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function CompressorIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M4 10.5h16M7 10.5V7.8A1.8 1.8 0 0 1 8.8 6h6.4A1.8 1.8 0 0 1 17 7.8v2.7M6.5 10.5v5.7A1.8 1.8 0 0 0 8.3 18h7.4a1.8 1.8 0 0 0 1.8-1.8v-5.7M9 14h6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="8" cy="18.5" r="1.2" fill="currentColor" />
      <circle cx="16" cy="18.5" r="1.2" fill="currentColor" />
    </svg>
  )
}

function statusToBadge(status: string): CompressorStatus {
  if (status === 'ON') return 'on'
  if (status === 'STANDBY') return 'standby'
  return 'off'
}

function formatNumber(value: number | null, fractionDigits: number = 1) {
  if (value == null || !Number.isFinite(value)) return '--'
  return new Intl.NumberFormat('it-IT', { maximumFractionDigits: fractionDigits }).format(value)
}

function formatDurationMinutes(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '--'

  const totalMinutes = Math.max(0, Math.round(value))
  const days = Math.floor(totalMinutes / (24 * 60))
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60)
  const minutes = totalMinutes % 60
  const parts: string[] = []

  if (days > 0) parts.push(`${days}g`)
  if (hours > 0 || days > 0) parts.push(`${hours}h`)
  parts.push(`${minutes}m`)

  return parts.join(' ')
}

function barWidth(value: number) {
  return `${Math.max(0, Math.min(100, value))}%`
}

export default function CompressorActivityPanel({
  items,
  loading = false,
  exportFileName = 'compressori',
  onExportData,
  onExportImage,
}: CompressorActivityPanelProps) {
  const cardRef = useRef<HTMLDivElement | null>(null)

  const handleExportData = () => {
    if (onExportData) {
      onExportData()
      return
    }
    if (typeof window === 'undefined') return

    const headers = [
      'id_compressore',
      'code',
      'name',
      'current_state',
      'dominant_state',
      'minutes_on',
      'minutes_standby',
      'minutes_off',
      'utilization_pct',
      'standby_pct',
      'off_pct',
      'avg_power_kw',
      'energy_kwh',
    ]
    const csv = [
      headers.join(';'),
      ...items.map((item) => [
        item.id_compressore,
        item.code,
        item.name,
        item.current_state,
        item.dominant_state,
        item.minutes_on ?? '',
        item.minutes_standby ?? '',
        item.minutes_off ?? '',
        item.utilization_pct ?? '',
        item.standby_pct ?? '',
        item.off_pct ?? '',
        item.avg_power_kw ?? '',
        item.energy_kwh ?? '',
      ].join(';')),
    ].join('\n')

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${exportFileName}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
  }

  const handleExportImage = async () => {
    if (onExportImage) {
      onExportImage()
      return
    }
    if (!cardRef.current) return
    await exportElementAsPng(cardRef.current, `${exportFileName}.png`)
  }

  return (
    <div ref={cardRef}>
      <Card className="overflow-hidden border-slate-200/80 shadow-[0_18px_45px_-28px_rgba(15,23,42,0.35)]">
      <CardHeader className="border-b border-slate-100 bg-gradient-to-br from-slate-50 to-white">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <div className="flex min-h-[2.25rem] items-center">
                <CardTitle className="inline-flex items-center gap-2 text-slate-900">
                  <span className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-slate-200/80 bg-white/80 text-slate-500 shadow-sm">
                    <CompressorIcon />
                  </span>
                  <span>Attivita compressori nel range</span>
                </CardTitle>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={handleExportData}
                  className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-white/90 text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
                  title="Esporta compressori come dati"
                >
                  <DownloadIcon />
                </button>
                <button
                  type="button"
                  onClick={() => void handleExportImage()}
                  className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-white/90 text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
                  title="Esporta compressori come immagine"
                >
                  <ImageIcon />
                </button>
              </div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white px-3 py-2 text-center shadow-sm">
              <div className="text-[11px] uppercase tracking-[0.16em] text-slate-400">Compressori</div>
              <div className="text-base font-semibold text-slate-900">{items.length}</div>
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {loading ? (
            <div className="space-y-3 p-4">
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
            </div>
          ) : items.length === 0 ? (
            <div className="flex h-48 items-center justify-center text-sm text-slate-500">
              Nessun dato compressori disponibile nel periodo richiesto.
            </div>
          ) : (
            <div className="divide-y divide-slate-100">
              {items.map((item) => (
                <div key={item.id_compressore} className="grid gap-4 p-4 xl:grid-cols-[1.3fr_1fr_0.9fr]">
                  <div className="grid gap-3 md:grid-cols-[minmax(140px,180px)_96px_210px] md:items-start md:gap-x-5">
                    <div className="space-y-2">
                      <div className="text-sm font-semibold text-slate-900">{item.code}</div>
                      <div className="text-sm text-slate-500">{item.name}</div>
                    </div>
                    <div className="md:pt-0.5 md:flex md:justify-start">
                      <StatusBadge status={statusToBadge(item.current_state)} />
                    </div>
                    <div className="md:pt-0.5 md:flex md:justify-start">
                      <span className="inline-flex whitespace-nowrap rounded-full bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-600">
                        Stato dominante: {item.dominant_state}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-xs font-medium uppercase tracking-[0.12em] text-slate-400">
                      <span>Utilizzo</span>
                      <span>{formatNumber(item.utilization_pct)}%</span>
                    </div>
                    <div className="h-3 overflow-hidden rounded-full bg-slate-100">
                      <div className="flex h-full">
                        <div className="bg-emerald-500" style={{ width: barWidth(item.utilization_pct) }} />
                        <div className="bg-amber-400" style={{ width: barWidth(item.standby_pct) }} />
                        <div className="bg-slate-300" style={{ width: barWidth(item.off_pct) }} />
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-2 text-xs text-slate-500">
                      <div>ON {formatDurationMinutes(item.minutes_on)}</div>
                      <div>STBY {formatDurationMinutes(item.minutes_standby)}</div>
                      <div>OFF {formatDurationMinutes(item.minutes_off)}</div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="text-[11px] uppercase tracking-[0.16em] text-slate-400">Potenza media</div>
                      <div className="mt-1 font-semibold text-slate-900">{formatNumber(item.avg_power_kw)} kW</div>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="text-[11px] uppercase tracking-[0.16em] text-slate-400">Energia</div>
                      <div className="mt-1 font-semibold text-slate-900">{formatNumber(item.energy_kwh)} kWh</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
