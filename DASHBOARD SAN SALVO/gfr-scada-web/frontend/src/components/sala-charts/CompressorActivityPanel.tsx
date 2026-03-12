import type { CompressorActivityItem } from '../../types/api'
import StatusBadge, { type CompressorStatus } from '../compressors/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card'
import Skeleton from '../ui/Skeleton'

interface CompressorActivityPanelProps {
  items: CompressorActivityItem[]
  loading?: boolean
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
}: CompressorActivityPanelProps) {
  return (
    <Card className="overflow-hidden border-slate-200/80 shadow-[0_18px_45px_-28px_rgba(15,23,42,0.35)]">
      <CardHeader className="border-b border-slate-100 bg-gradient-to-br from-slate-50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="text-slate-900">Attività compressori nel range</CardTitle>
            <p className="mt-1 text-sm text-slate-500">
              Minuti ON, STANDBY e OFF con utilizzo relativo del periodo selezionato.
            </p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-white px-3 py-2 text-right shadow-sm">
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
                    <span className="inline-flex rounded-full bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-600 whitespace-nowrap">
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
  )
}
