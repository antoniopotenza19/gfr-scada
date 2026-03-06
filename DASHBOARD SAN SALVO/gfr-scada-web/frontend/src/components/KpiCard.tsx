export default function KpiCard({ title, value }: { title: string; value: string }) {
  return (
    <div className="bg-white border border-slate-200 rounded p-4">
      <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{title}</div>
      <div className="mt-2 text-[32px] font-bold leading-[1.2] text-slate-900">{value}</div>
    </div>
  )
}
