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
        <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{title}</div>
        <div className="mt-2 text-[32px] font-bold leading-[1.2] text-slate-900">{value}</div>
        {hint ? <div className="mt-2 text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{hint}</div> : null}
      </CardContent>
    </Card>
  )
}
