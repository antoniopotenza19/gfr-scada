import { Card, CardContent } from './Card'

interface StatCardProps {
  title: string
  value: string
  hint?: string
}

export default function StatCard({ title, value, hint }: StatCardProps) {
  return (
    <Card>
      <CardContent>
        <div className="text-xs font-medium uppercase tracking-wide text-slate-500">{title}</div>
        <div className="mt-2 text-2xl font-semibold text-slate-900">{value}</div>
        {hint ? <div className="mt-2 text-xs text-slate-500">{hint}</div> : null}
      </CardContent>
    </Card>
  )
}
