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
  void totalKwhToday
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
      <Card>
        <CardContent className="p-4">
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">Portata Totale Impianto</div>
          <div className="mt-2 text-[32px] font-bold leading-[1.2] text-slate-900">{formatOneDecimal(totalFlowNm3)}</div>
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">Nm3/h realtime</div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="p-4">
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">Potenza Totale Impianto</div>
          <div className="mt-2 text-[32px] font-bold leading-[1.2] text-slate-900">{formatOneDecimal(totalPowerKw)}</div>
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">kW medi</div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="p-4 min-h-[116px]" />
      </Card>
      <Card>
        <CardContent className="p-4">
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">Sale In Allarme</div>
          <div className="mt-2 text-[32px] font-bold leading-[1.2] text-slate-900">{Math.max(0, Math.trunc(alertRooms))}</div>
          <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">anomalia o stale</div>
        </CardContent>
      </Card>
    </div>
  )
}
