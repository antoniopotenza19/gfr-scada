import { Card, CardContent } from './ui/Card'

interface KpiRowProps {
  totalFlowNm3: number
  totalPowerKw: number
  totalKwhToday: number
  alertRooms: number
}

function formatOneDecimal(value: number) {
  if (!Number.isFinite(value)) return '—'
  return value.toFixed(1)
}

export default function KpiRow({ totalFlowNm3, totalPowerKw, totalKwhToday, alertRooms }: KpiRowProps) {
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
      <Card>
        <CardContent className="p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Portata Totale</div>
          <div className="mt-2 text-2xl font-semibold text-slate-900">{formatOneDecimal(totalFlowNm3)}</div>
          <div className="text-xs text-slate-500">Nm3/h realtime</div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Potenza Totale</div>
          <div className="mt-2 text-2xl font-semibold text-slate-900">{formatOneDecimal(totalPowerKw)}</div>
          <div className="text-xs text-slate-500">kW medi</div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">kWh Oggi Totale</div>
          <div className="mt-2 text-2xl font-semibold text-slate-900">{formatOneDecimal(totalKwhToday)}</div>
          <div className="text-xs text-slate-500">kWh realtime</div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-slate-500">Sale In Allarme</div>
          <div className="mt-2 text-2xl font-semibold text-slate-900">{Math.max(0, Math.trunc(alertRooms))}</div>
          <div className="text-xs text-slate-500">anomalia o stale</div>
        </CardContent>
      </Card>
    </div>
  )
}

