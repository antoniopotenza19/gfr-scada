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
  height?: number
  tooltipLabel?: string
  valueFormatter?: (value: number) => string
}

export default function BarMetricChart({
  data,
  barColor = '#0f766e',
  xMode = 'auto',
  height = 320,
  tooltipLabel = 'Valore',
  valueFormatter,
}: BarMetricChartProps) {
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
  const formatTooltipValue = valueFormatter || ((value: number) => fullNumber(value))
  const formatAxisTime = (value: string) =>
    useMonthFormat
      ? new Date(value).toLocaleDateString('it-IT', { month: 'short', year: '2-digit' })
      : new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })

  return (
    <div style={{ width: '100%', height }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 12, right: 10, left: 4, bottom: 0 }}>
          <CartesianGrid stroke="#dbe4ee" strokeDasharray="3 6" vertical={false} />
          <XAxis
            dataKey="ts"
            tickFormatter={formatAxisTime}
            interval="preserveStartEnd"
            minTickGap={48}
            tickMargin={10}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 11, fill: '#64748b' }}
          />
          <YAxis
            width={64}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 11, fill: '#64748b' }}
            tickFormatter={(v: number) => compactNumber(v)}
          />
          <Tooltip
            cursor={{ fill: 'rgba(148, 163, 184, 0.08)' }}
            contentStyle={{
              borderRadius: 16,
              border: '1px solid rgba(226, 232, 240, 0.95)',
              boxShadow: '0 20px 45px rgba(15, 23, 42, 0.12)',
              background: 'rgba(255, 255, 255, 0.96)',
            }}
            labelStyle={{ color: '#334155', fontWeight: 600 }}
            labelFormatter={(v: string) => new Date(v).toLocaleString('it-IT')}
            formatter={(value: number) => [formatTooltipValue(Number(value)), tooltipLabel]}
          />
          <Bar dataKey="value" fill={barColor} radius={[8, 8, 0, 0]} maxBarSize={36} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
