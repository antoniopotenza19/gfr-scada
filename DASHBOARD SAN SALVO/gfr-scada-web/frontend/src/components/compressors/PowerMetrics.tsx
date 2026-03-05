interface PowerMetricItem {
  label: string
  unit: string
  value: string
  highlight?: boolean
}

interface PowerMetricsProps {
  items: PowerMetricItem[]
}

export default function PowerMetrics({ items }: PowerMetricsProps) {
  return (
    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
      {items.map((item) => (
        <div key={item.label} className="rounded-md border border-slate-200 bg-white px-3 py-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">{item.label}</div>
          <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-600">{item.unit}</div>
          <div className={`mt-1 text-2xl leading-none ${item.highlight ? 'font-semibold text-emerald-700' : 'font-medium text-slate-800'}`}>
            {item.value}
          </div>
        </div>
      ))}
    </div>
  )
}
