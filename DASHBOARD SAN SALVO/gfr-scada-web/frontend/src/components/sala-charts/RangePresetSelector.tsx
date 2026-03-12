import type { ChartRangeKey, RangePreset } from '../../constants/salaCharts'

interface RangePresetSelectorProps {
  presets: RangePreset[]
  activeKey: ChartRangeKey | null
  onSelect: (key: ChartRangeKey) => void
}

export default function RangePresetSelector({
  presets,
  activeKey,
  onSelect,
}: RangePresetSelectorProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {presets.map((preset) => {
        const active = preset.key === activeKey
        return (
          <button
            key={preset.key}
            type="button"
            onClick={() => onSelect(preset.key)}
            className={[
              'inline-flex min-h-[42px] items-center gap-2 rounded-full border px-4.5 py-2.5 text-sm font-semibold transition',
              active
                ? 'border-slate-900 bg-slate-900 text-white shadow-[0_10px_18px_-14px_rgba(15,23,42,0.9)]'
                : 'border-slate-200 bg-white text-slate-700 shadow-[0_8px_18px_-18px_rgba(15,23,42,0.3)] hover:border-slate-300 hover:bg-slate-50',
            ].join(' ')}
          >
            {preset.icon ? <span aria-hidden="true" className="inline-flex items-center justify-center text-base leading-none">{preset.icon}</span> : null}
            {preset.label}
          </button>
        )
      })}
    </div>
  )
}
