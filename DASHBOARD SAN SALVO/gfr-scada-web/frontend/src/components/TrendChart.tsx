import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from 'recharts'

interface DataPoint {
  ts: string
  value: number
}

interface TrendChartProps {
  data: DataPoint[]
}

export default function TrendChart({ data }: TrendChartProps) {
  if (!data || data.length === 0) {
    return <div className="h-64 flex items-center justify-center text-slate-500">No data</div>
  }
  return (
    <div style={{ width: '100%', height: 360 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="ts" tickFormatter={(v: string) => new Date(v).toLocaleTimeString()} />
          <YAxis />
          <Tooltip labelFormatter={(v: string) => new Date(v).toLocaleString()} />
          <Line type="monotone" dataKey="value" stroke="#0f172a" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
