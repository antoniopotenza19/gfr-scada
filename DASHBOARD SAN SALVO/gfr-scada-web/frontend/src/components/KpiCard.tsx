export default function KpiCard({ title, value }: { title: string; value: string }) {
  return (
    <div className="bg-white border border-slate-200 rounded p-4">
      <div className="text-sm text-slate-500">{title}</div>
      <div className="text-2xl font-semibold mt-2">{value}</div>
    </div>
  )
}
