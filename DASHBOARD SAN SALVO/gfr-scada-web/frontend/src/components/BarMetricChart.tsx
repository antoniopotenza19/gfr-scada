import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

interface DataPoint {
  ts: string
  value: number
}

interface BarMetricChartProps {
  data: DataPoint[]
  barColor?: string
  xMode?: 'auto' | 'time' | 'month'
}

export default function BarMetricChart({ data, barColor = '#0f766e', xMode = 'auto' }: BarMetricChartProps) {
  if (!data || data.length === 0) {
    return <div className="flex h-64 items-center justify-center text-sm text-slate-500">No data</div>
  }

  const firstTs = new Date(data[0].ts).getTime()
  const lastTs = new Date(data[data.length - 1].ts).getTime()
  const spanMs = Math.max(0, lastTs - firstTs)
  const useMonthFormat = xMode === 'month' || (xMode === 'auto' && spanMs >= 45 * 24 * 60 * 60 * 1000)
  const compactNumber = (value: number) =>
    new Intl.NumberFormat('it-IT', { notation: 'compact', maximumFractionDigits: 1 }).format(value)
  const fullNumber = (value: number) =>
    new Intl.NumberFormat('it-IT', { maximumFractionDigits: 2 }).format(value)
  const formatAxisTime = (value: string) =>
    useMonthFormat
      ? new Date(value).toLocaleDateString([], { month: '2-digit', year: '2-digit' })
      : new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })

  return (
    <div style={{ width: '100%', height: 320 }}>
      <ResponsiveContainer>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="ts"
            tickFormatter={formatAxisTime}
            interval="preserveStartEnd"
            minTickGap={48}
            tickMargin={10}
            tick={{ fontSize: 11 }}
          />
          <YAxis width={64} tick={{ fontSize: 11 }} tickFormatter={(v: number) => compactNumber(v)} />
          <Tooltip
            labelFormatter={(v: string) => new Date(v).toLocaleString()}
            formatter={(value: number) => [fullNumber(Number(value)), 'Valore']}
          />
          <Bar dataKey="value" fill={barColor} radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
