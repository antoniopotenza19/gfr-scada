import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from 'recharts'

interface DataPoint {
  ts: string
  value: number
}

interface TrendChartProps {
  data: DataPoint[]
}

export default function TrendChart({ data }: TrendChartProps) {
  const compactNumber = (value: number) =>
    new Intl.NumberFormat('it-IT', { notation: 'compact', maximumFractionDigits: 1 }).format(value)
  const fullNumber = (value: number) =>
    new Intl.NumberFormat('it-IT', { maximumFractionDigits: 2 }).format(value)

  const formatAxisTime = (value: string) =>
    new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })

  if (!data || data.length === 0) {
    return <div className="h-64 flex items-center justify-center text-slate-500">No data</div>
  }
  return (
    <div style={{ width: '100%', height: 360 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="ts"
            tickFormatter={formatAxisTime}
            interval="preserveStartEnd"
            minTickGap={42}
            tickMargin={10}
            tick={{ fontSize: 11 }}
          />
          <YAxis width={64} tick={{ fontSize: 11 }} tickFormatter={(v: number) => compactNumber(v)} />
          <Tooltip
            labelFormatter={(v: string) => new Date(v).toLocaleString()}
            formatter={(value: number) => [fullNumber(Number(value)), 'Valore']}
          />
          <Line type="monotone" dataKey="value" stroke="#0f172a" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
